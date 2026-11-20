# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, LongType
import re

# COMMAND ----------

dbutils.widgets.text("env","")
dbutils.widgets.text("secret_scope", "kv-mx-data-adf-inges-dev")
dbutils.widgets.text("table_name","")
dbutils.widgets.text("query", "")

# COMMAND ----------

env = dbutils.widgets.get("env").lower()
secret_scope= dbutils.widgets.get("secret_scope")
table_name = dbutils.widgets.get("table_name").strip()
query = dbutils.widgets.get("query").strip()

if not table_name:
    raise ValueError("Widget 'table_name' cannot be empty.")

query = f"SELECT * FROM {table_name}" if not query else query

# COMMAND ----------

# Snowflake connection details
sfUser = "FESFSVCETL"
sfPassword = dbutils.secrets.get(scope=secret_scope, key="cloud-snowflake-svc-prd-password")
sfAccount = "farahexperiences.west-europe.azure.snowflakecomputing.com"
sfDatabase = "FEEDWPROD"
sfWarehouse = "DPM_WH"

# Snowflake read options
options = {
    "sfUrl": sfAccount,
    "sfUser": sfUser,
    "sfPassword": sfPassword,
    "sfDatabase": sfDatabase,
    "sfWarehouse": sfWarehouse,
    "parallelism": "8"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# COMMAND ----------

# ---------------------------------------------------------
# Function to clean column names and cast Decimal(38,0) → LongType
# ---------------------------------------------------------
def clean_column_names_and_types(df):
    renamed_cols = []
    for field in df.schema:
        col_name = field.name
        new_col_name = col_name.strip('"')  # remove extra quotes if any
        column_expr = col(f"`{col_name}`")  # safe reference

        # Cast Decimal(38,0) to LongType
        if isinstance(field.dataType, DecimalType) and field.dataType.precision == 38 and field.dataType.scale == 0:
            column_expr = column_expr.cast(LongType())

        # Rename only if needed
        if col_name != new_col_name:
            column_expr = column_expr.alias(new_col_name)

        renamed_cols.append(column_expr)

    return df.select(*renamed_cols)

# COMMAND ----------

# ---------------------------------------------------------
# Function to process a single query/table
# ---------------------------------------------------------
def process_single_dataset(dataset_name, query, env, options):
    try:
        df = spark.read.format("snowflake").options(**options).option("query", query).load()
    except Exception as e:
        raise Exception(f"Issue reading from Snowflake: {e}")

    cleaned_df = clean_column_names_and_types(df)

    output_path = f"abfss://ingestion@stmxdatadatalake{env}01.dfs.core.windows.net/datastore_snowflake/{dataset_name}/"
    cleaned_df.write.mode("overwrite").format("parquet").save(output_path)
    print(f" Written dataset: {dataset_name} → {output_path}")

# COMMAND ----------

dataset_name = f"dataset_{table_name.replace('.', '_')}"  # keep consistent naming
process_single_dataset(dataset_name, query, env, options)
