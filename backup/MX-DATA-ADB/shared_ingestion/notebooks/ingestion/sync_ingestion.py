# Databricks notebook source
# MAGIC %run ../../shared_code/common_functions/ingestion_functions

# COMMAND ----------

"""
This code is used to sync the Databricks catalog with
the data stored in ingestion container in the data lake.

<root_path>/datastore_a/dataset_i/**.parquet
"""

environment = dbutils.widgets.get("environment")

ingestion_catalog_name = "ingestion_" + environment

root_directory = f"abfss://ingestion@stmxdatadatalake{environment}01.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Determine which schemas and tables we should have

# COMMAND ----------

fullload_datasets_in_storage, incremental_datasets_in_storage, cdc_dataset_in_storage, masking_dataset_in_storage = get_datasets_in_storage(root_directory, ignore_tables_ingestion)
all_datasets = fullload_datasets_in_storage | incremental_datasets_in_storage | cdc_dataset_in_storage

schemas_in_storage = {dataset.schema_name() for dataset in all_datasets}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Determine which schemas and tables we have now

# COMMAND ----------

tables_in_catalog = get_tables_in_catalog(ingestion_catalog_name, ignore_tables_ingestion)
schemas_in_catalog = get_schemas_in_catalog(ingestion_catalog_name)

# # COMMAND ----------

# # MAGIC %md
# # MAGIC ### Determine which schemas and tables to create and to remove

# # COMMAND ----------

schemas_to_create = schemas_in_storage - schemas_in_catalog
schemas_to_remove = schemas_in_catalog - schemas_in_storage - ignore_lnd_schemas_delete

print(f"Schemas we should have: ({len(schemas_in_storage)})")
print(f"Schemas we have:        ({len(schemas_in_catalog)})")
print(f"Schemas to create:      ({len(schemas_to_create)}): {schemas_to_create}")
print(f"Schemas to remove:      ({len(schemas_to_remove)}): {schemas_to_remove}")

# # COMMAND ----------

fulload_tables_to_create = fullload_datasets_in_storage - tables_in_catalog
cdc_tables_to_create = cdc_dataset_in_storage - tables_in_catalog
# incremental tables will be created by the incremental workflow

tables_to_remove = tables_in_catalog - fullload_datasets_in_storage - cdc_dataset_in_storage - incremental_datasets_in_storage - masking_dataset_in_storage

# For set types + is replaced by |
# Refresh only the existing fullload and cdc tables
tables_to_refresh = (fullload_datasets_in_storage | cdc_dataset_in_storage) - fulload_tables_to_create - cdc_tables_to_create - incremental_datasets_in_storage

print(f"Number of fullload tables to create:  {len(fulload_tables_to_create)}")
pprint(fulload_tables_to_create)

print(f"Number of cdc tables to create:  {len(cdc_tables_to_create)}")
pprint(cdc_tables_to_create)

print(f"\n\nNumber of tables to remove:  {len(tables_to_remove)}")
pprint(tables_to_remove)

print(f"\n\nNumber of tables to refresh: {len(tables_to_refresh)}")
# pprint(tables_to_refresh)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop tables and schemas we don't need anymore

# COMMAND ----------

failures_tables_to_remove = {}
if tables_to_remove:
    for dataset in tables_to_remove:
        print(f"Removing {dataset}")
        try:
            drop_table(ingestion_catalog_name, dataset)
        except Exception as e:
            print("    Failed!")
            failures_tables_to_remove[dataset] = e
else:
    print("No tables to remove")


print_failures(failures_tables_to_remove)

# COMMAND ----------

failures_schemas_to_remove = {}
if schemas_to_remove:
    for schema_name in schemas_to_remove:
        print(f"Removing {schema_name}")
        try:
            drop_schema(ingestion_catalog_name, schema_name)
        except Exception as e:
            print("    Failed!")
            failures_schemas_to_remove[schema_name] = e
else:
    print("No schemas to remove")

print_failures(failures_schemas_to_remove)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create schemas and tables we don't have yet

# COMMAND ----------

# # Create schemas
failures_schemas_to_create = {}
if schemas_to_create:
    for schema_name in schemas_to_create:
        print(f"Creating {schema_name}")
        try:
            create_schema(ingestion_catalog_name, schema_name, root_directory)
        except Exception as e:
            print("    Failed!")
            failures_schemas_to_create[schema_name] = e
else:
    print("No new schemas needed")

print_failures(failures_schemas_to_create)

# COMMAND ----------

failures_tables_to_create = {}
if fulload_tables_to_create:
    for dataset in fulload_tables_to_create:
        print(f"Creating {dataset}")
        try:
            create_table(ingestion_catalog_name, dataset, 'PARQUET')
        except Exception as e:
            print("    Failed!")
            failures_tables_to_create[dataset] = e
else:
    print("No new parquet tables needed")

if cdc_tables_to_create:
    for dataset in cdc_tables_to_create:
        print(f"Creating {dataset}")
        try:
            create_table(ingestion_catalog_name, dataset, 'DELTA')
        except Exception as e:
            print("    Failed!")
            failures_tables_to_create[dataset] = e
else:
    print("No new delta tables needed")

print_failures(failures_tables_to_create)

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

failures_tables_to_refresh = {}
if tables_to_refresh:
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {dataset: executor.submit(refresh_table, ingestion_catalog_name, dataset) for dataset in tables_to_refresh}

        for dataset, future in futures.items():
            try:
                future.result()
                # print(f"Table: {dataset} Refreshed!")
            except Exception as e:
                print(f"Table: {dataset} Failed!")
                failures_tables_to_refresh[dataset] = str(e)
else:
    print("No refresh needed")

print_failures(failures_tables_to_refresh)

# COMMAND ----------

# MAGIC %md
# MAGIC # Fail workflow if there was an failure

# COMMAND ----------

# # COMMAND ----------

cancel_runs_after_first_two("sync_ingestion")


if (
    failures_schemas_to_create
    or failures_tables_to_create
    or failures_tables_to_refresh
    or failures_tables_to_remove
    or failures_schemas_to_remove
):
    raise Exception("There was at least one issue with this sync")
