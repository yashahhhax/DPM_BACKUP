# Databricks notebook source
# MAGIC %md
# MAGIC #####Contains all the base functions required

# COMMAND ----------

dbutils.widgets.text("env","")
dbutils.widgets.text("usecase","")
dbutils.widgets.text("current_job_run_id","")

# COMMAND ----------

# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #####Base function call to receive the param values

# COMMAND ----------

try:
    notebook_vars = [
        'env',
        'usecase',
        'current_job_run_id'
    ]
    env, usecase, current_job_run_id = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

slv_table = f"transformation_{env}.slv_shared.prep_lkp_source_system"
gld_table = f"transformation_{env}.gld_shared.dwh_lkp_source_system"

# COMMAND ----------

# Get the current user for audit columns like created_by, modified_by
current_user_df = spark.sql("SELECT current_user() AS user")
current_user_value = current_user_df.collect()[0]["user"]
print(current_user_value)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp, lit

source_system_df = spark.table(slv_table)
final_source_system_df = source_system_df.withColumn("CREATED_DATETIME", current_timestamp()).withColumn("CREATED_BY", lit(current_user_value))

# COMMAND ----------

final_source_system_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(gld_table)

# COMMAND ----------

updated_records_count = final_source_system_df.count()
new_records_count = 0

# COMMAND ----------

gold_stats = {
    "gld_table": gld_table,
    "dim_new_records_count": new_records_count,
    "dim_updated_records_count": updated_records_count,
    "fact_inserted_records_count": 0,
    "fact_updated_records_count": 0
}

log_gld_table_stats(
    usecase= usecase,
    table_name=gld_table,
    gold_stats=gold_stats,
    current_job_run_id=current_job_run_id
) 
