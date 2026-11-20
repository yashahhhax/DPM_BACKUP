# Databricks notebook source
# MAGIC %md
# MAGIC #####Contains all the base functions required

# COMMAND ----------

dbutils.widgets.text("env", "")
dbutils.widgets.text("tablename", "")
dbutils.widgets.text("current_job_run_id", "")

# COMMAND ----------

# DBTITLE 1,Run base_functions
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #####Base function call to receive the param values

# COMMAND ----------

try:
    notebook_vars = [
        'tablename',
        'env',
        'current_job_run_id'
    ]
    tablename, env, current_job_run_id = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Contains expected schemas for all the tables

# COMMAND ----------

# MAGIC %run ../../config/schema_config

# COMMAND ----------

schema_var_name = f"dim_{tablename}_schema"
dim_schema = globals().get(schema_var_name)
print(dim_schema)

# Build DDL string
if dim_schema is None:
    raise ValueError(f"Schema variable {schema_var_name} not found")

# Build DDL string dynamically
dim_schema_ddl = ", ".join(f"{col} {dtype}" for col, dtype in dim_schema.items())

print(dim_schema_ddl)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Contains travel box tables metadata required to implement scd2

# COMMAND ----------

# MAGIC %run ../../config/table_config

# COMMAND ----------

# Import necessary libraries
import json
from pyspark.sql.functions import concat_ws, col, current_timestamp, lit, trim, current_user, coalesce
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

config = drr_shared_objects_config[tablename]

source_table_ref = config["source_table"]

target_table_ref = config["target_table"]

surrogate_keys = config["surrogate_keys"]

primary_key_cols = config["primary_key_cols"]

scd_columns = config["scd_columns"]


# COMMAND ----------

# MAGIC %md
# MAGIC #####Call to SCD2 function

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable
from datetime import datetime
from functools import reduce
from pyspark.sql.utils import AnalysisException

# Execute SCD Type 2 logic (common for both cases)
status, new_records_count, updated_records_count = implement_scd_type2(source_table_ref, target_table_ref, surrogate_keys, primary_key_cols, scd_columns)

print(status)
print(new_records_count)
print(updated_records_count) 

# COMMAND ----------

add_last_updated_tag(target_table_ref)

# COMMAND ----------

gold_stats = {
    "gld_table": target_table_ref,
    "dim_new_records_count": new_records_count,
    "dim_updated_records_count": updated_records_count,
    "fact_inserted_records_count": 0,
    "fact_updated_records_count": 0
}

log_gld_table_stats(
    usecase = "shared",
    current_job_run_id = current_job_run_id,
    table_name = target_table_ref,
    gold_stats = gold_stats
) 

# COMMAND ----------


