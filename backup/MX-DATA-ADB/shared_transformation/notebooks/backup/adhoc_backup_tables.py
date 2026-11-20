# Databricks notebook source
# DBTITLE 1,create env widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_prefix", "")
dbutils.widgets.text("backup_location", "")
dbutils.widgets.text("exclude_schemas", "")
dbutils.widgets.text("exclude_tables","")
dbutils.widgets.text("adhoc_backup_date","")

# COMMAND ----------

# DBTITLE 1,import table_backup_utils
# MAGIC %run ./table_backup_utils

# COMMAND ----------

# DBTITLE 1,fetch env widgets value
from datetime import datetime, time, timezone

catalog_name = dbutils.widgets.get("catalog_name").strip().lower()
schema_prefix = dbutils.widgets.get("schema_prefix").strip().lower()
backup_to_location_path = dbutils.widgets.get("backup_location").strip()
adhoc_backup_date = dbutils.widgets.get("adhoc_backup_date").strip()

backup_timestamp = (
    datetime.fromisoformat(adhoc_backup_date)
    .replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=0,
        tzinfo=timezone.utc
    )
    .isoformat(timespec="seconds")
    if adhoc_backup_date else None
)

print(backup_timestamp)

# Read exclude schemas
exclude_schemas_str = dbutils.widgets.get("exclude_schemas").strip().lower()
exclude_schemas = (
    [schema.strip() for schema in exclude_schemas_str.split(",") if schema.strip()]
    if exclude_schemas_str
    else []
)

# Read exclude tables
exclude_tables_str = dbutils.widgets.get("exclude_tables").strip().lower()
exclude_tables = (
    [table.strip() for table in exclude_tables_str.split(",") if table.strip()]
    if exclude_tables_str
    else []
)

# Validate mandatory parameters
if not catalog_name:
    raise ValueError("catalog_name is required.")

if not schema_prefix:
    raise ValueError("schema_prefix is required.")

if not backup_to_location_path:
    raise ValueError("backup_location is required.")

# Logging
logger.info(f"Catalog Name              : {catalog_name}")
logger.info(f"Schema Prefix             : {schema_prefix}")
logger.info(f"Business Date             : {adhoc_backup_date}")
logger.info(f"Backup Location           : {backup_to_location_path}")
logger.info(f"Exclude Schemas           : {exclude_schemas}")
logger.info(f"Exclude Tables            : {exclude_tables}")


# COMMAND ----------

input_table_name = get_tables_from_catalog(catalog_name, schema_prefix, exclude_schemas, exclude_tables)
print(input_table_name)

tables_paths = generate_table_paths(input_table_name, backup_to_location_path, backup_timestamp)

logger.info(tables_paths)

for idx, (_, paths) in enumerate(tables_paths.items()):
    logger.info(f"Backing up datable {idx + 1} of {len(tables_paths)}")

    backup_data(paths["input_table"], paths["backup_table"])




# COMMAND ----------


