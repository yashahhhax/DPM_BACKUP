# Databricks notebook source
# MAGIC %run ../../shared_code/common_functions/ingestion_functions

# COMMAND ----------

# Overview
#
# This notebook keeps the bronze layer of the transformation catalog in
# sync with the ingestion catalog:
#
#   <root_path>/datastore_a/dataset_i/**.parquet
#
# For each table in the ingestion catalog (under an lnd_* schema), a
# corresponding view (vw_<table>) is maintained in the transformation
# catalog under the matching brz_* schema.
#
# Steps:
#   1. Ensure bronze schemas exist for every landing schema.
#   2. Diff ingestion tables vs. transformation views to find views to
#      add / refresh / remove.
#   3. Detect which existing views have drifted from their source table's
#      schema (bulk comparison, not per-view DESCRIBE).
#   4. Apply all changes in parallel.
#   5. Fail the job (and cancel duplicate concurrent runs) if anything
#      went wrong.

environment = dbutils.widgets.get("environment")

ingestion_catalog_name = "ingestion_" + environment
transformation_catalog_name = "transformation_" + environment
root_directory = f"abfss://ingestion@stmxdatadatalake{environment}01.dfs.core.windows.net"
root_directory_transformation = f"abfss://transformation@stmxdatadatalake{environment}01.dfs.core.windows.net"

# Bronze-side equivalents of the bronze views we ignore on the ingestion side
# (e.g. helper/staging tables that shouldn't get their own vw_ view).
ignore_tables_transformation = ["vw_" + table for table in ignore_bronze_views]

# COMMAND ----------

# Step 1: Ensure bronze schemas exist
#
# For every lnd_<x> schema in the ingestion catalog, there should be a
# corresponding brz_<x> schema in the transformation catalog.

ingestion_schemas = get_schemas_in_catalog(ingestion_catalog_name)
ingestion_schemas = [schema.replace("lnd_", "brz_") for schema in ingestion_schemas if "lnd_" in schema]

transformation_schemas = get_schemas_in_catalog(transformation_catalog_name)
transformation_bronze_schemas = [schema for schema in transformation_schemas if "brz_" in schema]

schemas_to_add = set(ingestion_schemas) - set(transformation_bronze_schemas)
schemas_to_remove = set(transformation_bronze_schemas) - set(ingestion_schemas) - ignore_brz_schemas_delete

print(f"Schemas to add: {len(schemas_to_add)}")
print(f"Schemas to remove: {len(schemas_to_remove)}")
print(f"Schemas to add: {schemas_to_add}")
print(f"Schemas to remove: {schemas_to_remove}")

for schema in schemas_to_add:
    create_schema(transformation_catalog_name, schema, root_directory_transformation)

# Schema removal is intentionally disabled - dropping a schema is destructive
# and should be a deliberate manual action, not part of an automated sync.
# for schema in schemas_to_remove:
#     drop_schema(transformation_catalog_name, schema)

# COMMAND ----------

# Step 2: Build the view diff (add / refresh-candidates / remove)

def transform_schema_name(schema_name: str) -> str:
    """Map an ingestion schema name (lnd_x) to its transformation equivalent (brz_x)."""
    return schema_name.replace("lnd_", "brz_")


# All tables in the ingestion catalog, annotated with their target bronze schema name.
tables_in_ingestion_catalog = [
    Dataset(
        datastore_name=table.datastore_name,
        name=table.name,
        location=table.location,
        new_schema_name=transform_schema_name(table.schema_name())
    )
    for table in get_tables_in_catalog(ingestion_catalog_name, ignore_bronze_views)
]

# All existing tables/views in brz_* schemas of the transformation catalog.
identifiers_in_transformation_catalog = [
    table for table in get_tables_in_catalog(transformation_catalog_name, ignore_tables_transformation)
    if "brz_" in table.schema_name()
]

# Index both sides by their "transformation-side" identifier (new_schema.vw_table)
# so we can diff them directly.
ingestion_dict = {table.full_table_name_with_new_schema(): table for table in tables_in_ingestion_catalog}
transformation_dict = {table.full_table_name(): table for table in identifiers_in_transformation_catalog}

# Views that exist on the ingestion side but not yet in transformation -> create.
identifiers_to_add = set(ingestion_dict) - set(transformation_dict)

# Views that exist in transformation but no longer have a source table -> candidates for removal.
identifiers_to_remove = set(transformation_dict) - set(ingestion_dict)

# All views that currently have a matching source table -> candidates for refresh
# (actual refresh need is determined in Step 4 via schema comparison).
identifiers_to_refresh = set(ingestion_dict) - identifiers_to_add

views_to_add = [ingestion_dict[key] for key in identifiers_to_add]
views_to_refresh = [ingestion_dict[key] for key in identifiers_to_refresh]

print(f"Views to add: {len(views_to_add)}")
print(f"Views to add: {views_to_add}")
print(f"Views to refresh (candidates): {len(views_to_refresh)}")

# COMMAND ----------

# Step 3: Resolve views_to_remove
#
# An identifier slated for removal might be a TABLE (not a VIEW) - e.g. if
# someone manually materialized something in a brz_ schema. We only want to
# drop objects that are actually VIEWs, and never touch protected schemas.
#
# Resolved via a single bulk information_schema.tables query rather than
# one spark.catalog.getTable() call per identifier.

candidate_remove_keys = [
    key for key in identifiers_to_remove
    if key.split(".")[0] not in ignore_brz_schemas_delete
]

if candidate_remove_keys:
    schemas_involved = {key.split(".")[0] for key in candidate_remove_keys}
    table_type_map = get_table_types(transformation_catalog_name, schemas_involved)
else:
    table_type_map = {}

views_to_remove = [
    transformation_dict[key]
    for key in candidate_remove_keys
    if table_type_map.get(key) == "VIEW"
]

print(f"Views to remove: {len(views_to_remove)}")
print(f"Views to remove: {views_to_remove}")

# COMMAND ----------

# Step 4: Detect schema drift to determine which views actually need refreshing
#
# Instead of running DESCRIBE on each source table and each view
# individually (2 x N round trips), fetch the full column metadata for all
# relevant schemas in just two queries (one per catalog) and compare in
# memory.

source_schemas = {v.schema_name() for v in views_to_refresh}
target_schemas = {v.new_schema_name for v in views_to_refresh}

# {schema.table -> [(column_name, full_data_type), ...]} for source tables.
source_columns_map = get_columns_map(ingestion_catalog_name, source_schemas)

# Same, for the existing bronze views.
target_columns_map = get_columns_map(transformation_catalog_name, target_schemas)

# Object types for existing bronze-side identifiers, so we can skip refreshing
# anything that isn't actually a VIEW (mirrors the original refresh_view's
# tableType check).
target_table_type_map = get_table_types(transformation_catalog_name, target_schemas)

views_needing_refresh = []
for v in views_to_refresh:
    source_key = v.full_table_name()
    target_key = f"{v.new_schema_name}.vw_{v.table_name()}"

    # If the target identifier exists but is not a VIEW (e.g. someone created
    # a real table with the same name), don't touch it.
    existing_type = target_table_type_map.get(target_key)
    if existing_type is not None and existing_type != "VIEW":
        print(f"Skipping refresh for {target_key} as it is a {existing_type}, not a VIEW.")
        continue

    source_cols = source_columns_map.get(source_key)
    target_cols = target_columns_map.get(target_key)

    if source_cols is None:
        # Source table no longer has columns / doesn't exist - shouldn't
        # normally happen since it came from get_tables_in_catalog, but skip
        # defensively rather than erroring.
        continue

    # Refresh if the view doesn't exist yet, or its columns/types differ
    # from the current source table.
    if target_cols is None or source_cols != target_cols:
        views_needing_refresh.append(v)

print(f"Views needing refresh: {len(views_needing_refresh)} out of {len(views_to_refresh)}")
print(views_needing_refresh)

# COMMAND ----------

# Step 5: Apply changes in parallel
#
# Each operation (create / refresh / drop) is independent at the metadata
# level, so they're run concurrently via run_parallel. Logging happens
# inside the underlying create_view / refresh_view / drop_view
# functions in ingestion_functions.

failures_views_to_add = run_parallel(
    lambda view: create_view(ingestion_catalog_name, transformation_catalog_name, view),
    views_to_add,
    max_workers=16,
    label="Views to add"
)

failures_views_to_refresh = run_parallel(
    lambda view: refresh_view(ingestion_catalog_name, transformation_catalog_name, view),
    views_needing_refresh,
    max_workers=32,
    label="Views to refresh"
)

failures_views_to_remove = run_parallel(
    lambda view: drop_view(transformation_catalog_name, view.full_table_name()),
    views_to_remove,
    max_workers=16,
    label="Views to remove"
)

print(failures_views_to_add)
print(failures_views_to_refresh)
print(failures_views_to_remove)

# COMMAND ----------

# Step 6: Fail the workflow if anything went wrong
#
# Also cancel any older overlapping runs of this same job so failures don't
# cause a pile-up of queued retries.

cancel_runs_after_first_two("sync_ingestion")

if (
    failures_views_to_add
    or failures_views_to_refresh
    or failures_views_to_remove
):
    raise Exception("There was at least one issue with this sync")