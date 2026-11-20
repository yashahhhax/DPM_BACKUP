# Databricks notebook source
# DBTITLE 1,import widgets
dbutils.widgets.text("env","")
dbutils.widgets.text("usecase", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("alert_type", "")
dbutils.widgets.text("etl_job_alerts_required", "", "Etl Job Alerts Required")

# COMMAND ----------

# DBTITLE 1,required flag code
refresh_flag = dbutils.widgets.get("etl_job_alerts_required").strip().lower()

# Treat empty input as "yes"
if refresh_flag == "":
    refresh_flag = "yes"

# Define normalized acceptable values
yes_values = [ "y","yes"]
no_values  = [ "n", 'no']

# Conditional logic
if refresh_flag in no_values:
    print(f"Skipping notebook execution since etl alerts refresh is not required. (Input: '{refresh_flag}')")
    dbutils.notebook.exit("Skipped as per widget setting")

elif refresh_flag in yes_values:
    print(f"Proceeding with etl alerts as requested. (Input: '{refresh_flag}')")

else:
    print(f"⚠️ Invalid widget value '{refresh_flag}'. Expected Y/Yes or N/No.")
    dbutils.notebook.exit("Skipped due to invalid input.")


# COMMAND ----------

# MAGIC %run ../common_functions/base_functions

# COMMAND ----------

from datetime import datetime

try:
    notebook_vars = ['env','usecase', 'job_run_id','alert_type']
    
    # Unpack with defaults as None
    env, usecase, job_run_id, alert_type = get_notebook_params(*notebook_vars, verbose=True)
    usecase=usecase.lower()

except Exception as e:
    print(f"[ERROR] {str(e)}")
    raise


# COMMAND ----------

# MAGIC %run ./logger_utility_functions

# COMMAND ----------

# MAGIC %run ./etl_util_alert_functions

# COMMAND ----------

# MAGIC %run ./etl_alert_config

# COMMAND ----------

#Auto-detect workspace name and type based on workspace URL
from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient()

# Try to get workspace URL from Spark config (most reliable)
try:
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
except Exception:
    workspace_url = workspace_client.config.host or "unknown"

# Clean trailing slashes if any
workspace_url = workspace_url.strip().rstrip("/")

# Define a lookup map for all known workspaces
workspace_url_map = {
    # --- Ingestion Workspaces ---
    "adb-2766497291955675.15.azuredatabricks.net": "dbw-mx-data-adb-inges-dev-01",
    "adb-1199415078036138.18.azuredatabricks.net": "dbw-mx-data-adb-inges-idev-01",
    "adb-2731791389375139.19.azuredatabricks.net": "dbw-mx-data-adb-inges-prd-01",
    "adb-1321531737024345.5.azuredatabricks.net": "dbw-mx-data-adb-inges-preprd-01",
    "adb-1628851157633194.14.azuredatabricks.net": "dbw-mx-data-adb-inges-uat-01",

    # --- Transformation Workspaces ---
    "adb-485279261696768.8.azuredatabricks.net": "dbw-mx-data-adb-trans-dev-01",
    "adb-2424669970410431.11.azuredatabricks.net": "dbw-mx-data-adb-trans-idev-01",
    "adb-1645531006591813.13.azuredatabricks.net": "dbw-mx-data-adb-trans-prd-01",
    "adb-635866415826582.2.azuredatabricks.net": "dbw-mx-data-adb-trans-preprd-01",
    "adb-125701397626662.2.azuredatabricks.net": "dbw-mx-data-adb-trans-uat-01",

    # --- ML Workspaces ---
    "adb-3821540461636682.2.azuredatabricks.net": "dbw-mx-data-adb-ml-dev-01",
    "adb-998748609691736.16.azuredatabricks.net": "dbw-mx-data-adb-ml-idev-01",
    "adb-1630099358760621.1.azuredatabricks.net": "dbw-mx-data-adb-ml-prd-01",
    "adb-3859238561111997.17.azuredatabricks.net": "dbw-mx-data-adb-ml-preprd-01",
    "adb-4291857358528928.8.azuredatabricks.net": "dbw-mx-data-adb-ml-uat-01",

    # --- UC Admin ---
    "adb-723762768397429.9.azuredatabricks.net": "dbw-mx-data-adb-uc-admin-prd-01",
}

# Match the workspace name from URL
workspace_name = workspace_url_map.get(workspace_url, "unknown-workspace")

# Infer workspace type (based on name)
if "inges" in workspace_name:
    workspace_type = "ingestion"
elif "trans" in workspace_name:
    workspace_type = "transformation"
elif "ml" in workspace_name:
    workspace_type = "ml"
elif "uc-admin" in workspace_name:
    workspace_type = "uc-admin"
else:
    workspace_type = "unknown"

print(f"Environment    : {env}")
print(f"Workspace URL  : {workspace_url}")
print(f"Workspace Name : {workspace_name}")
print(f"Workspace Type : {workspace_type}")

# COMMAND ----------

# Build webhook_url dynamically
if usecase == "shared":
    webhook_url = f"{env}_shared_trans_webhook_url"
else:
    webhook_url = f"{env}_{usecase}_webhook_url"

print(webhook_url)

# Get the actual value from globals()
webhook_url = globals()[webhook_url]

# COMMAND ----------

print(webhook_url)

# COMMAND ----------

job_name = None
job_id = None
latest_run_id = job_run_id

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema
schema = StructType([
    StructField("job_name", StringType(), True),
    StructField("job_id", StringType(), True),
    StructField("tag", LongType(), True),
    StructField("latest_run_id", LongType(), True),
])

# Create data with None values
data = [(None, None, None, int(job_run_id))]

# Create DataFrame
df = spark.createDataFrame(data, schema)

df.show()


# COMMAND ----------

job_details_df = get_job_run_details(df, spark)
display(job_details_df)

# COMMAND ----------

# etl job start notification
if alert_type.lower() == "start":
    send_pipeline_start_notification(job_details_df, webhook_url, usecase, env, workspace_name)

# COMMAND ----------

if alert_type.lower() == "failure":
    # Get first row from DataFrame
    row = job_details_df.first()

    # Parse values
    job_name = row['job_name']
    job_run_id = row['job_run_id']
    start_time = row['start_time']
    run_page_url = row['run_page_url']
    tasks_details = row['tasks_details']

    # Extract task_key, result_state, run_id, and state_message safely
    task_status_list = []
    for task in tasks_details:
        try:
            task_key = task['task_key']
            result_state_raw = task['state']['result_state']
            result_state = result_state_raw.split('.')[-1] if result_state_raw else 'UNKNOWN'
            run_id = task['run_id']  
            state_message = task['state']['state_message']
            # Get task-specific start_time (handle missing field)
            task_start_time = task['start_time'] if 'start_time' in task else 0
            task_status_list.append({
                "task_key": task_key,
                "result_state": result_state,
                "run_id": run_id,
                "state_message": state_message,
                "task_start_time": task_start_time
            })
        except Exception as e:
            print(f"Skipping task due to error: {e}")

    # Convert to pandas DataFrame
    task_status_df = pd.DataFrame(task_status_list)

    # Ensure 'run_id' is numeric (convert if needed)
    task_status_df['run_id'] = pd.to_numeric(task_status_df['run_id'], errors='coerce')
    task_status_df['task_start_time'] = pd.to_numeric(task_status_df['task_start_time'], errors='coerce')
    
    # Keep only the latest run per task_key (highest task_start_time)
    latest_runs_df = (
        task_status_df
        .sort_values(['task_key', 'task_start_time'], ascending=[True, False])
        .drop_duplicates(subset=['task_key'], keep='first')
    )

    # Filter rows where result_state is FAILED
    failed_df = latest_runs_df[latest_runs_df['result_state'].str.upper() == 'FAILED']

    if failed_df.empty:
        print("No failed tasks found. Skipping error details retrieval.")
        failed_task_run_id = None
        error_details = None
    else:
        # Get the earliest failure by task_start_time (root cause)
        min_start_time = failed_df['task_start_time'].min()
        failed_min_df = failed_df[failed_df['task_start_time'] == min_start_time]
        failed_min_df.display()

        # Get the run_id from the earliest failed task
        failed_task_run_id = failed_min_df.iloc[0]['run_id'] if not failed_min_df.empty else None
        print(failed_task_run_id)

        # Fetch run output and error details
        workspace_client = WorkspaceClient()
        error_details = get_run_output(failed_task_run_id, workspace_client=workspace_client)
        print(error_details)
        
        # etl job failure notification
        send_pipeline_failure_notification(
            job_name,
            job_run_id,
            start_time,
            run_page_url,
            failed_task_run_id,
            error_details,
            webhook_url,
            usecase,
            env,
            workspace_name
        )

# COMMAND ----------

# etl job success notification
if alert_type.lower() == "status":
    send_pipeline_status_notification(job_details_df, webhook_url, usecase, env, workspace_name)
