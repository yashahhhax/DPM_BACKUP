# Databricks notebook source
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_prefix", "")
dbutils.widgets.text("backup_location", "")
dbutils.widgets.text("exclude_schemas", "")
dbutils.widgets.text("exclude_tables","")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name").strip().lower()
schema_prefix = dbutils.widgets.get("schema_prefix").strip().lower()
backup_to_location_path = dbutils.widgets.get("backup_location").strip()
exclude_schemas_str = dbutils.widgets.get("exclude_schemas").strip().lower()
exclude_schemas = [s.strip() for s in exclude_schemas_str.split(",")] if exclude_schemas_str else None
exclude_tables_str = dbutils.widgets.get("exclude_tables").strip().lower()
exclude_tables = [s.strip() for s in exclude_tables_str.split(",")] if exclude_tables_str else None

if not (catalog_name and schema_prefix and backup_to_location_path):
    raise ValueError("Please provide all required parameters.")

# COMMAND ----------

# MAGIC %run ./table_backup_utils

# COMMAND ----------

input_table_name = get_tables_from_catalog(catalog_name, schema_prefix, exclude_schemas, exclude_tables)
print(input_table_name)
tables_paths = generate_table_paths(input_table_name, backup_to_location_path)

for idx, (_, paths) in enumerate(tables_paths.items()):
    logger.info(f"Backing up datable {idx + 1} of {len(tables_paths)}")

    backup_data(paths["input_table"], paths["backup_table"])

logger.info(f"Backup of catalog `{catalog_name}` completed.")
