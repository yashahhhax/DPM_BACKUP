# Databricks notebook source
# DBTITLE 1,Widgets
# dbutils.widgets.text("env","")
# dbutils.widgets.text("table_name","")

# COMMAND ----------

# DBTITLE 1,Call base function
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,get_notebook_params
from datetime import datetime
 
try:
    notebook_vars = ['env', 'table_name']
   
    # Unpack with defaults as None
    env, table_name = get_notebook_params(*notebook_vars, verbose=True)
 
except Exception as e:
    print(f"[ERROR] {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,add_last_updated_tag
table = f"transformation_{env}.gld_shared.{table_name}"
 
# print(table)
add_last_updated_tag(table)

# COMMAND ----------

# DBTITLE 1,trigger_databricks_job_by_name
trigger_databricks_job_by_name('workflow_orchestration')
