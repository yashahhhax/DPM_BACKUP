# Databricks notebook source
from datetime import datetime, timezone
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# COMMAND ----------

def get_tables_from_catalog(
    catalog_name: str,
    schema_prefix: str,
    exclude_schemas: Optional[list[str]] = None,
    exclude_tables: Optional[list[str]] = None
) -> list:
    """Get all tables in a catalog.

    We want to skip all the views from each catalog and only back up the managed
    and external tables.

    Args:
        catalog_name (str): The catalog name.
        schema_prefix (str): The schema prefix to filter tables.
        exclude_schemas (list[str], optional): List of schema names to exclude.
        exclude_tables (list[str], optional): List of full table names (schema.table) to exclude.

    Returns:
        list: A list of table names in the catalog.

    """
    tables = spark.sql(
        f"SELECT table_schema, table_name FROM {catalog_name}.information_schema.tables"
    ).collect()

    full_table_paths = [
        f"{catalog_name}.{tuple(row)[0]}.{tuple(row)[1]}"
        for row in tables
        if tuple(row)[0].startswith(schema_prefix)
        and (exclude_schemas is None or tuple(row)[0] not in exclude_schemas)
        and (exclude_tables is None or f"{tuple(row)[0]}.{tuple(row)[1]}" not in exclude_tables)
    ]

    logger.info(f"Tables in catalog `{catalog_name}`: {full_table_paths}")

    return full_table_paths

# COMMAND ----------

def generate_table_paths(
    table_names: list[str],
    backup_to_location_path: str,
    backup_timestamp: Optional[str] = None
) -> dict[str, dict[str, str]]:
    """
    Generate the input and output table paths.

    Args:
        table_names (list[str]): A list of fully qualified table names to back up.
        backup_to_location_path (str): The external location (e.g., abfss://...) to back up the tables to.
        backup_timestamp (str, optional): A timestamp string to use in the path. If not provided, the current UTC time is used.

    Returns:
        dict[str, dict[str, str]]: A dictionary mapping each table name to its input and backup paths.
            Example:
            {
                "catalog.schema.table": {
                    "input_table": "catalog.schema.table",
                    "backup_table": "abfss://.../databricks/catalog/schema/table/{timestamp}"
                }
            }
    """
    # Generate timestamp if not provided
    if backup_timestamp is None:
        backup_timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Build paths
    table_paths = {
        table_name: {
            "input_table": table_name,
            "backup_table": f"{backup_to_location_path}/databricks/{table_name.replace('.', '/')}/{backup_timestamp}",
        }
        for table_name in table_names
    }

    logger.info(f"Backup timestamp: {backup_timestamp}")
    logger.info(f"The following tables will be backed up: {table_paths}")

    return table_paths


# COMMAND ----------

def backup_data(input_table_name: str, output_table_path: str) -> None:
    """
    Back up a Delta or managed table in Databricks to an external location as Parquet.

    Args:
        input_table_name (str): The fully qualified name of the input table (e.g., catalog.schema.table).
        output_table_path (str): The destination path to save the backup (e.g., DBFS or cloud storage URI).
        spark (SparkSession): Active Spark session.
    """
    # Read the source table into a DataFrame
    # NOTE:  We use writing to parquet instead of clone, as cloning is not supported on tables with column masking
    df_to_back_up = spark.table(input_table_name)

    # Write the DataFrame to the specified external path in Parquet format
    # "overwrite" mode ensures any existing data at the path is replaced
    df_to_back_up.write.format("parquet").mode("overwrite").save(output_table_path)

    logger.info(f"Table `{input_table_name}` has been successfully backed up to `{output_table_path}`.")