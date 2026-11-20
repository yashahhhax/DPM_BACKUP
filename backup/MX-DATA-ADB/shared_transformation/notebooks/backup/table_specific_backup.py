# Databricks notebook source
dbutils.widgets.text('catalog_name', "")
dbutils.widgets.text('schema_name', "")
dbutils.widgets.text('table_name', "")
dbutils.widgets.text('business_date', "")
dbutils.widgets.text('backup_location', "abfss://daily@stmxdatabackupdev01.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %run ./table_backup_utils

# COMMAND ----------

catalog_name = dbutils.widgets.get('catalog_name').strip().lower()
schema_name = dbutils.widgets.get('schema_name').strip().lower()
table_name = dbutils.widgets.get('table_name').strip().lower()
business_date = dbutils.widgets.get('business_date').strip()
backup_to_location_path = dbutils.widgets.get('backup_location').strip()

if not (catalog_name and schema_name and table_name and backup_to_location_path):
    raise ValueError("Please provide all required parameters.")

input_table_name = f"{catalog_name}.{schema_name}.{table_name}"
backup_timestamp = (
    datetime.fromisoformat(business_date).isoformat(timespec='seconds')
    if business_date else None
)

logger.info(f"catalog_name: {catalog_name}")
logger.info(f"schema_name: {schema_name}")
logger.info(f"table_name: {table_name}")
logger.info(f"business_date: {business_date}")
logger.info(f"backup_timestamp: {backup_timestamp}")
logger.info(f"backup_to_location_path: {backup_to_location_path}")
logger.info(f"input_table_name: {input_table_name}")

# COMMAND ----------


tables_paths = generate_table_paths([input_table_name], backup_to_location_path, backup_timestamp)

logger.info(tables_paths)

for idx, (_, paths) in enumerate(tables_paths.items()):
    logger.info(f"Backing up datable {idx + 1} of {len(tables_paths)}")

    backup_data(paths["input_table"], paths["backup_table"])