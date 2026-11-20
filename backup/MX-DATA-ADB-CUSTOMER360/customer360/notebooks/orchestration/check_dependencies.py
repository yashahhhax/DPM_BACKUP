# Databricks notebook source
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

from datetime import datetime,timedelta
from ast import literal_eval

# COMMAND ----------

# dbutils.widgets.text("env", "")
# env = dbutils.widgets.get("env")


# dbutils.widgets.text("tables", "")
# tables = dbutils.widgets.get("tables")

# dbutils.widgets.text("is_check_required", "n")
# buffer_days = dbutils.widgets.get("is_check_required")

# dbutils.widgets.text("tag_buffer_days", "")

# COMMAND ----------

# DBTITLE 1,init parameters
try:
    notebook_vars = [
        'env','tables','tag_buffer_days','is_check_required'
    ]
    env,tables,tag_buffer_days,is_check_required= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,exit notebook if check not required
if str(is_check_required).lower() == 'no':
    print("Notebook exited: is_check_required is set to 'no'.")
    dbutils.notebook.exit("is_check_required is set to 'no'.")

# COMMAND ----------

# DBTITLE 1,set catalog
# Set catalog
transformation_catalog = f"transformation_{env}"

spark.sql(f"USE CATALOG {transformation_catalog}")

# COMMAND ----------

# DBTITLE 1,Parse tables parameter
# Read widget value for tables
tables_str = tables.strip()
# Parse safely into Python list
tables_raw = literal_eval(tables_str) if tables_str else []

# Support both formats:
#   case 1: ["schema.table1", "schema.table2"]
#   case 2: [["schema.table1", ""], ["schema.table2", "ts_col"]]
tables_list = []
for entry in tables_raw:
    if isinstance(entry, (list, tuple)):
        table_name = entry[0]
        ts_col = entry[1].strip() if len(entry) > 1 and entry[1] else None
    else:
        table_name = str(entry)
        ts_col = None
    tables_list.append((table_name, ts_col))

print(tables_list)

# COMMAND ----------

# DBTITLE 1,call function
date = (datetime.now()).strftime('%Y-%m-%d') if tag_buffer_days == '' else (datetime.now() - timedelta(days=int(tag_buffer_days))).strftime('%Y-%m-%d')
print(f"Checking tables refreshed on or after {date}\n")
check_table_refresh(transformation_catalog, tables_list,date)
