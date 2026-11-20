# Databricks notebook source
# DBTITLE 1,Widgets
# dbutils.widgets.text("env","")
# dbutils.widgets.text("table_name","")
# dbutils.widgets.text("is_failed","")

# COMMAND ----------

# DBTITLE 1,Call base function
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,get_notebook_params
from datetime import datetime
from pyspark.sql.functions import lower
 
try:
    notebook_vars = ['env', 'table_name','is_failed']
   
    # Unpack with defaults as None
    env, table_name,is_failed = get_notebook_params(*notebook_vars, verbose=True)
    is_failed = (is_failed.lower() == 'y') if is_failed is not None else False
 
except Exception as e:
    print(f"[ERROR] {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,add_last_updated_tag
table = f"transformation_{env}.plt_customer360.{table_name}"

if is_failed:
    # print(table)
    add_last_updated_tag(table,True)
else:
    # print(table)
    add_last_updated_tag(table) 


# COMMAND ----------

# DBTITLE 1,trigger_databricks_job_by_name
trigger_databricks_job_by_name('workflow_orchestration')
