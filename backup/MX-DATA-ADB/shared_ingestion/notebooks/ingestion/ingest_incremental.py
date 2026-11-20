# Databricks notebook source
# DBTITLE 1,Imports
from datetime import datetime, timedelta
import json

# COMMAND ----------

# DBTITLE 1,Merge Functions
def get_watermark_condition(watermark_cols, i):
    if i == 0:
        return f"target.{watermark_cols[i]} < source.{watermark_cols[i]}"

    result = " AND ".join(
        [f"target.{watermark_cols[j]} = source.{watermark_cols[j]}" for j in range(i)]
    )
    result += f" AND target.{watermark_cols[i]} < source.{watermark_cols[i]}"
    return result

def merge_tables(pk: str, watermark_col: str, source_table: str, target_table: str):
    """
    Perform a MERGE operation between a source and target table based on:
      - Primary key (can be compound)
      - Watermark column(s) (can be single or multiple, comma-separated)

    Supports lexicographical comparison for multiple watermark columns.
    """

    # ---------------- PK handling ----------------
    pk_cols = [col.strip() for col in pk.split(',')]
    pk_join_condition = ' AND '.join([f'target.{col} = source.{col}' for col in pk_cols])
    pk_partition_cols = ', '.join(pk_cols)

    # ---------------- Watermark handling ----------------
    watermark_cols = [c.strip() for c in watermark_col.split(',')]

    # ORDER BY for ROW_NUMBER()
    watermark_order_by = ', '.join([f"{c} DESC" for c in watermark_cols])

    if len(watermark_cols) == 1:
        # Single watermark (unchanged behavior)
        wm_less_condition = f"target.{watermark_cols[0]} < source.{watermark_cols[0]}"
        wm_equal_condition = f"target.{watermark_cols[0]} = source.{watermark_cols[0]}"
    else:
        # Composite watermark (lexicographical comparison)
        wm_less_condition = " OR ".join(
            f"({get_watermark_condition(watermark_cols, i)})"
            for i in range(len(watermark_cols))
        )

        wm_equal_condition = " AND ".join(
            [f"target.{c} = source.{c}" for c in watermark_cols]
        )

    # ---------------- Target existence / schema checks ----------------
    target_type = get_identifier_type(target_table)

    # Anything that exists but is not a managed table (EXTERNAL, VIEW, ...) is dropped
    if target_type is not None and target_type != 'MANAGED':
        print(f"{target_table} exists as {target_type}. Dropping it before creating a managed table.")
        drop_table_or_view(target_table)
        target_type = None  # Reset type as it no longer exists

    if target_type is None:
        print(f"Table {target_table} does not exist. Creating table now.")
        create_target_table(source_table, target_table)
    # If it is a managed table, check for schema mismatch
    elif not compare_table_schema(source_table, target_table):
        print("Table schema mismatch. Failing workflow.")
        raise Exception("Schema mismatch detected: retrigger ingestion pipeline in ADF with full_load = true")

    # ---------------- Column & Hash logic ----------------
    target_cols = get_columns(target_table)
    all_columns = [col for col in target_cols if col != 'adf_load_time']
    hash_expr = f"hash({', '.join([f'source.{col}' for col in all_columns])})"
    target_hash_expr = f"hash({', '.join([f'target.{col}' for col in all_columns])})"

    update_set = ', '.join([f'target.{col} = source.{col}' for col in target_cols])
    insert_cols = ', '.join(target_cols)
    insert_vals = ', '.join([f'source.{col}' for col in target_cols])

    # ---------------- MERGE SQL ----------------
    merge_sql = f"""
    WITH latest_source AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {pk_partition_cols}
                ORDER BY {watermark_order_by}, adf_load_time DESC
            ) AS rn
        FROM {source_table}
    )
    MERGE INTO {target_table} AS target
    USING (
        SELECT * FROM latest_source WHERE rn = 1
    ) AS source
    ON {pk_join_condition}

    WHEN MATCHED AND ({wm_less_condition})
        THEN UPDATE SET {update_set}

    WHEN MATCHED AND ({wm_equal_condition}) AND {target_hash_expr} != {hash_expr}
        THEN UPDATE SET {update_set}

    WHEN NOT MATCHED
        THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
    """

    # ---------------- Execute ----------------
    df = spark.sql(merge_sql)
    df.display()
    print(f"Merge completed between {source_table} and {target_table}.")

    # Return the merge metrics
    return df.first().asDict()

# COMMAND ----------

# DBTITLE 1,Utility Functions
def create_temp_view(source_path, source_temp_view):
    df = spark.read.format("parquet").load(source_path)
    df.createOrReplaceTempView(source_temp_view)
    print(f"Temp view {source_temp_view} created from location {source_path}")

def drop_table_or_view(identifier_name: str):
    """
    Drops the specified table or view if it exists.
    Does nothing if it doesn't exist. Real errors (e.g. permissions) are raised.
    """
    identifier_type = get_identifier_type(identifier_name)

    if identifier_type is None:
        print(f"{identifier_name} does not exist. Nothing to drop.")
        return

    if identifier_type == 'VIEW':
        print(f"Dropping VIEW {identifier_name}")
        spark.sql(f"DROP VIEW {identifier_name}")
    else:
        print(f"Dropping {identifier_type} table {identifier_name}")
        spark.sql(f"DROP TABLE {identifier_name}")

def get_identifier_type(identifier_name: str) -> str | None:
    """
    Check if the given identifier exists in the Spark catalog and return its type.

    Parameters:
    identifier_name (str): The full name of the identifier in the format 'catalog.schema.name'.

    Returns:
    str | None: 'MANAGED', 'EXTERNAL' or 'VIEW' if it exists, None otherwise.
    """
    try:
        table_meta = spark.catalog.getTable(identifier_name)
        return table_meta.tableType  # Returns 'MANAGED', 'EXTERNAL', 'VIEW', etc.
    except Exception as e:
        # If getTable fails (e.g., TABLE_OR_VIEW_NOT_FOUND), the identifier does not exist.
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            return None
        else:
            # Re-raise other unexpected errors
            print(f"An unexpected error occurred while checking identifier existence for {identifier_name}: {e}")
            raise e

def compare_table_schema(source_table: str, target_table: str):
    source_schema = sorted(get_columns(source_table))
    target_schema = sorted(get_columns(target_table))
    return source_schema == target_schema

def create_target_table(source_table: str, target_table: str):
    """
    Create the target table by selecting all data from the source table.
    """
    create_table_sql = f"""
    CREATE TABLE {target_table} AS
    SELECT * FROM {source_table}
    LIMIT 0
    """
    # Execute the create table statement
    spark.sql(create_table_sql)
    print(f"Target table {target_table} created with the same schema as {source_table}.")

def get_columns(table_name: str):
    """
    Helper function to get the column names of a table.
    """
    # Fetch the schema of the table
    df = spark.table(table_name)
    columns = df.columns
    return columns

def delete_files_older_than(location: str, days: int):
    """
    Delete files older than the specified number of days, but always
    keep the newest file so the folder never becomes empty.

    Parameters:
    location (str): The path to the external location.
    days (int): The number of days to check for older files.
    """
    try:
        cutoff_ms = (datetime.now() - timedelta(days=days)).timestamp() * 1000

        all_files = dbutils.fs.ls(location)
        if not all_files:
            print(f"No files found in {location}")
            return

        newest_file = max(all_files, key=lambda file: file.modificationTime)

        for file in all_files:
            if file.modificationTime < cutoff_ms and file.path != newest_file.path:
                dbutils.fs.rm(file.path)
                print(f"Deleted file: {file.path}")

    except Exception as e:
        print(f"Error deleting files in location {location}: {e}")
        raise

def create_schema(catalog_name: str, schema_name: str, root_directory: str) -> None:
    if schema_name.startswith("lnd_"):
        root_directory = f"{root_directory}/landing"
    elif schema_name.startswith("brz_"):
        root_directory = f"{root_directory}/bronze"
    else:
        raise ValueError(f"Schema {schema_name} does not start with lnd_ or brz_")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name} MANAGED LOCATION '{root_directory}/{schema_name}'")

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("ingestion_schema", "test")
dbutils.widgets.text("table_name", "test")
dbutils.widgets.text("pk", "col1")
dbutils.widgets.text("timestamp_col", "timestamp")
dbutils.widgets.text("is_fullload", "false")

# COMMAND ----------

# DBTITLE 1,Variable Setup
environment = dbutils.widgets.get("env")
schema_name = dbutils.widgets.get("ingestion_schema")
table_name = dbutils.widgets.get("table_name")
pk = dbutils.widgets.get("pk") # pk can now be "col1" or "col1, col2, col3"
watermark_col = dbutils.widgets.get("timestamp_col")
is_fullload = dbutils.widgets.get("is_fullload").lower()

landing_schema = "lnd_" + schema_name

root_directory = f"abfss://ingestion@stmxdatadatalake{environment}01.dfs.core.windows.net"
source_path = f'{root_directory}/datastore_{schema_name}/dataset_{table_name}'

ingestion_catalog_name = "ingestion_" + environment

source_temp_view=f"{table_name}_tempview"
target_table=f"{ingestion_catalog_name}.{landing_schema}.{table_name}"

# COMMAND ----------

# DBTITLE 1,Main Execution
# Create Schema if not existed
create_schema(ingestion_catalog_name, landing_schema, root_directory)

#Create temp view on table source to ease the further process
create_temp_view(source_path, source_temp_view)

if is_fullload == "true":
    print(f"Full load requested. Dropping and recreating table {target_table}.")
    drop_table_or_view(target_table)
    create_target_table(source_temp_view, target_table)

merge_result = merge_tables(
    pk=pk, 
    watermark_col=watermark_col,  
    source_table=source_temp_view,
    target_table=target_table
)

# Delete older files than 7 days (keeps at least the newest file)
delete_files_older_than(source_path, 7)

exit_output = {
    "status": "success",
    "target_table": target_table,
    "num_affected_rows": merge_result["num_affected_rows"],
    "num_inserted_rows": merge_result["num_inserted_rows"],
    "num_updated_rows": merge_result["num_updated_rows"],
    "num_deleted_rows": merge_result["num_deleted_rows"]
}
dbutils.notebook.exit(json.dumps(exit_output))
