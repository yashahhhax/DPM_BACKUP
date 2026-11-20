# Databricks notebook source
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

from datetime import datetime,timedelta

# COMMAND ----------

# dbutils.widgets.text("env", "")
# env = dbutils.widgets.get("env")

# dbutils.widgets.text("current_job_run_id", "")
# env = dbutils.widgets.get("current_job_run_id")

# COMMAND ----------

# DBTITLE 1,init parameters
try:
    notebook_vars = [
        'env','current_job_run_id'
    ]
    env,current_job_run_id= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,check trigger type
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
job_run = w.jobs.get_run(run_id=int(current_job_run_id))

# run type possible values : ONE_TIME, PERIODIC, CRON, FILE_ARRIVAL, TABLE, CONTINUOUS
run_type = job_run.trigger.value if job_run.trigger else 'UNKNOWN'

print(f"Trigger type: {run_type}")

# COMMAND ----------

# DBTITLE 1,chek latest refreshment
if run_type in ['PERIODIC','CRON']:
    last_runs = list(w.jobs.list_runs(job_id=job_run.job_id, limit=2)); last_run = last_runs[1] if len(last_runs) > 1 else None
    if last_run:
        last_run_date = datetime.fromtimestamp(last_run.start_time / 1000).date()
        current_date = datetime.now().date()
        is_last_run_today = last_run_date == current_date
else:
    is_last_run_today = False

# COMMAND ----------

# DBTITLE 1,skip the run if executed today
if is_last_run_today:
    # raise Exception("Skipping today's run as it was already executed.")
    w.jobs.cancel_run(run_id=int(current_job_run_id))
    dbutils.notebook.exit("Cancelled current job run as today's run was already executed.")
else:
    print("Continue with today's run.")
