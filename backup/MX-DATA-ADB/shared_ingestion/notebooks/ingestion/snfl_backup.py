# Databricks notebook source
dbutils.widgets.text("env","")
dbutils.widgets.text("secret_scope", "kv-mx-data-adf-inges-dev")
dbutils.widgets.text("tables","")

# COMMAND ----------

# MAGIC %run ./snfl_backup_queries

# COMMAND ----------

env = dbutils.widgets.get("env")
secret_scope= dbutils.widgets.get("secret_scope")
tables = dbutils.widgets.get("tables")

# COMMAND ----------

from pyspark.sql import SparkSession

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

from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, LongType
import re

# COMMAND ----------

# tables_list = ['feedwprod.public.dwh_dim_product', 'feedwprod.public.dwh_dim_date']

# COMMAND ----------

import ast

# Read widget value for tables
tables_str = dbutils.widgets.get("tables").strip()

# Parse safely into Python list
tables_list = ast.literal_eval(tables_str) if tables_str else []

# Add dynamic queries for each table in tables_list
for tbl in tables_list:
    # Create dataset name by replacing . with _
    query_name = f"dataset_{tbl.replace('.', '_')}"
    
    # Form the query
    query_sql = f"SELECT * FROM {tbl}"
    
    # Add to queries dictionary
    queries[query_name] = query_sql

# Debug print
if tables_list:
    print("Generated queries from widget tables:")
    for k, v in queries.items():
        print(k, "->", v[:50] + "...")
else:
    print("No tables passed in widget → will use queries dict from imported notebook")


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
def process_single_dataset(name, query_or_table, env, options):
    try:
        df = spark.read.format("snowflake").options(**options).option("query", query_or_table).load()
    except Exception as e:
        raise Exception(f"Issue reading from Snowflake: {e}")

    cleaned_df = clean_column_names_and_types(df)

    output_path = f"abfss://ingestion@stmxdatadatalake{env.lower()}01.dfs.core.windows.net/datastore_snowflake/{name}/"
    cleaned_df.write.mode("overwrite").format("parquet").save(output_path)
    print(f" Written dataset: {name} → {output_path}")

# COMMAND ----------

# ---------------------------------------------------------
# Auto-run logic based on widget value
# ---------------------------------------------------------
if not tables_list:  
    # Case 1: No tables passed in widget → run all queries from queries dict
    for name, query in queries.items():  # queries dict imported from your other notebook
        process_single_dataset(name.lower(), query, env, options)

else:  
    # Case 2: Tables list provided → run SELECT * for each table in the list
    for tbl in tables_list:
        sql_query = f"SELECT * FROM {tbl}"
        query_name = f"dataset_{tbl.replace('.', '_')}"  # keep consistent naming
        process_single_dataset(query_name.lower(), sql_query, env, options)
