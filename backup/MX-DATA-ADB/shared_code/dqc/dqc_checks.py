# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx "typing-extensions>=4.13.0"

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("env", "")
dbutils.widgets.text("layer", "")
dbutils.widgets.text("usecase", "")
dbutils.widgets.text("tablename", "")
dbutils.widgets.text("obj_type", "") 
dbutils.widgets.text("task_run_id", "")

# COMMAND ----------

# DBTITLE 1,Call base functions
# MAGIC %run ../common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,Notebook parameters
try:
    notebook_vars = [
        'env',
        'layer',
        'usecase',
        'tablename',
        'obj_type',
        'task_run_id'
    ]
    env, layer, usecase, tablename, obj_type,task_run_id = get_notebook_params(*notebook_vars, verbose=True)

except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Create schema
catalog = f"transformation_{env}"
# print(catalog)
schema = "etl_audit"
dqc_table = "adb_dqc_history"
dqc_full_table_name = f"{catalog}.{schema}.{dqc_table}"
# print(dqc_full_table_name)

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,DQC Table creation
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {dqc_full_table_name} (
        dqc_date DATE,
        usecase STRING,
        layer STRING,
        object_name STRING,
        run_id BIGINT,
        column_values MAP<STRING, STRING>,
        dq_errors ARRAY<STRUCT<
            name: STRING,
            message: STRING,
            col_name: STRING,
            filter: STRING,
            function: STRING,
            run_time: TIMESTAMP,
            user_metadata: MAP<STRING, STRING>
        >>,
        dq_warnings ARRAY<STRUCT<
            name: STRING,
            message: STRING,
            col_name: STRING,
            filter: STRING,
            function: STRING,
            run_time: TIMESTAMP,
            user_metadata: MAP<STRING, STRING>
        >>,
        dqc_refresh_timestamp TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (dqc_date, usecase)
""")

# COMMAND ----------

# DBTITLE 1,Initiating workspace client & DQ Engine
# Import core DQx classes needed for profiling, rule generation, and DLT integration
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.engine import DQEngine
try:
    from databricks.labs.dqx.config import ExtraParams
except:
    from databricks.labs.dqx.rule import ExtraParams

# Databricks SDK for workspace interaction
from databricks.sdk import WorkspaceClient

# Utility libraries
import yaml
import json

# PySpark imports for schema, functions, and transformations
from pyspark.sql.types import *
from pyspark.sql.functions import current_date, lit, struct, map_from_arrays, col, array

# Python date/time utilities
from datetime import datetime, date
from decimal import Decimal

# Initialize a WorkspaceClient for Databricks interaction
ws_client = WorkspaceClient()
print(ws_client) 

# Customize DQ column names,By default, DQx uses 'errors' and 'warnings' for results
custom_column_names = {
    "errors": "dq_errors",
    "warnings": "dq_warnings"
}

# Configure extra parameters for DQx using ExtraParams class
extra_parameters = ExtraParams(result_column_names=custom_column_names)

# Initialize the DQEngine with the workspace client and extra parameters
dq_engine = DQEngine(ws_client, extra_params=extra_parameters)


# COMMAND ----------

# DBTITLE 1,Import  DQC Checks config file
# Load the YAML file with the checks
with open(f"../../config/dqc_{layer}_checks.yaml", "r") as file:
    checks_config = yaml.safe_load(file)

# COMMAND ----------

# DBTITLE 1,apply_table_checks
def apply_table_checks(table_name, input_df):
    all_tables = []

    # Handle dims/facts with optional nested tables
    for section_key in ["dims","facts","dims_omni", "facts_omni","facts_clymb","dims_clymb","facts_ig","dims_ig"]:
        for item in checks_config.get(section_key, []):
            if "tables" in item:
                all_tables.extend(item["tables"])  # structure 2
            elif "name" in item and "checks" in item:
                all_tables.append(item)  # structure 1

    # Handle top-level tables key (structure 3)
    all_tables.extend(checks_config.get("tables", []))

    # Match table
    table_checks = None
    for table in all_tables:
        if table.get("name") == table_name:
            table_checks = table.get("checks", [])
            break

    if table_checks is None:
        raise ValueError(f"No checks found for table: {table_name}")

    # Run checks
    try:
        return dq_engine.apply_checks_by_metadata(input_df, table_checks)
    except Exception as e:
        raise RuntimeError(f"Failed to apply checks on {table_name}: {e}")
 

# COMMAND ----------

# DBTITLE 1,extract_tables_from_config & run_dq_checks
from pyspark.sql.functions import current_date, col, lit, array, map_from_arrays,expr
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, TimestampType, MapType
from datetime import datetime

def extract_tables_from_config(config_section, tablename_filter=None):
    tables = []

    if not isinstance(config_section, list):
        raise ValueError("Invalid or empty config section.")

    # Scenario 1: nested structure
    if "tablename" in config_section[0] and "tables" in config_section[0]:
        for group in config_section:
            group_name = group.get("tablename")
            if tablename_filter and group_name != tablename_filter:
                continue
            for t in group.get("tables", []):
                t = t.copy()
                t["group_tablename"] = group_name
                tables.append(t)

    # Scenario 2: flat list
    elif "name" in config_section[0] and "checks" in config_section[0]:
        for t in config_section:
            if tablename_filter and t.get("name") != tablename_filter:
                continue
            t = t.copy()
            t["group_tablename"] = None
            tables.append(t)

    else:
        raise ValueError("Unsupported config format detected.")

    return tables

def normalize_check_columns(tables):
    """
    Normalize `col_name` to `col_names` for consistency.
    """
    for table in tables:
        for check in table.get("checks", []):
            args = check.get("check", {}).get("arguments", {})
            if "col_name" in args and "col_names" not in args:
                args["col_names"] = [args.pop("col_name")]
    return tables

def run_dq_checks():
    # displayHTML(f"<h4>Starting DQC with layer=<b>{layer}</b>, obj_type=<b>{obj_type}</b>, tablename=<b>{tablename}</b></h4>")
    
    if layer == "brz":
        dims_config = checks_config.get(obj_type, [])
        if not dims_config:
            raise ValueError(f"No config found for obj_type='{obj_type}' in bronze layer.")

        # Use tablename filter ONLY if it's provided and not empty
        table_filter = tablename if tablename else None
        tables = extract_tables_from_config(dims_config, tablename_filter=table_filter)

        # print(f"Bronze Layer: Tables selected (after filtering): {len(tables)}")
        # displayHTML(f"<p style='color:gray'>Tables picked: {', '.join([t['name'] for t in tables])}</p>")
    
    else:
        # Non-bronze layers: flat list of tables
        flat_tables = checks_config.get("tables", [])
        if not flat_tables:
            raise ValueError("No 'tables' key found in config for non-brz layer.")

        tables = [table for table in flat_tables if table.get("name") == tablename]
        if not tables:
            raise ValueError(f"No matching table found in config for: {tablename}")

    print("Selected Tables:", [t["name"] for t in tables])

    run_id = dbutils.widgets.get("task_run_id")
    print(f"Provided task_run_id: {run_id}")

    for table in tables:
        table_name = table["name"]
        if layer == "brz":
            table_abs_name = f"transformation_{env}.{table_name}"
        else:
            table_abs_name = f"transformation_{env}.{layer}_{usecase}.{table_name}"

        # displayHTML(f"<p style='color:blue'>Running DQC for: {table_name}</p>")

        try:
            input_df = spark.table(table_abs_name)
            result_df = apply_table_checks(table_name, input_df)

            failed_df = result_df.filter(
                (result_df["dq_errors"].isNotNull()) | (result_df["dq_warnings"].isNotNull())
            )

            if failed_df.count() > 0:
                value_cols = [c for c in failed_df.columns if not c.startswith("dq")]

                failed_df = failed_df.withColumn(
                    "column_values",
                    map_from_arrays(
                        lit(value_cols),
                        array(*[col(c).cast("string") for c in value_cols])
                    )
                )

                issue_struct_type = ArrayType(
                    StructType([
                        StructField("name", StringType(), True),
                        StructField("message", StringType(), True),
                        StructField("col_name", StringType(), True),
                        StructField("filter", StringType(), True),
                        StructField("function", StringType(), True),
                        StructField("run_time", TimestampType(), True),
                        StructField("user_metadata", MapType(StringType(), StringType()), True)
                    ])
                )

                enriched_dqc_df = failed_df.select(
                    current_date().alias("dqc_date"),
                    lit(usecase).alias("usecase"),
                    lit(layer).alias("layer"),
                    lit(table_abs_name).alias("object_name"),
                    lit(run_id).cast("long").alias("run_id"),
                    col("column_values"),
                    expr("""transform(dq_errors, x ->
                                named_struct(
                                'name', x.name,
                                'message', x.message,
                                'col_name', element_at(x.columns, 1),
                                'filter', x.filter,
                                'function', x.function,
                                'run_time', x.run_time,
                                'user_metadata', x.user_metadata))""").cast(issue_struct_type).alias("dq_errors"),
                    expr("""transform(dq_warnings, x ->
                                named_struct(
                                'name', x.name,
                                'message', x.message,
                                'col_name', element_at(x.columns, 1),
                                'filter', x.filter,
                                'function', x.function,
                                'run_time', x.run_time,
                                'user_metadata', x.user_metadata))""").cast(issue_struct_type).alias("dq_warnings"),
                    lit(datetime.now()).alias("dqc_refresh_timestamp")
                )

                (enriched_dqc_df
                 .write
                 .format("delta")
                 .mode("append")
                 .partitionBy("dqc_date", "usecase")
                 .saveAsTable(dqc_full_table_name)
                )

                # display(enriched_dqc_df)

        except Exception as e:
            print(f" Failed to process {table_abs_name}: {e}")
            raise
 

# COMMAND ----------

# DBTITLE 1,Call run_dq_checks
# Run DQ checks
# print("Running DQ checks unconditionally.")
run_dq_checks()

# COMMAND ----------


