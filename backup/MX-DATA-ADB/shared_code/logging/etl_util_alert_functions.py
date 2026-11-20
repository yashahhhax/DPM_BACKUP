# Databricks notebook source
from databricks.sdk import WorkspaceClient

def get_run_output(run_id: int, workspace_client: WorkspaceClient) -> str:
    """
    Get the run output message for a given run ID using WorkspaceClient.

    Args:
        run_id (int): ID of the Databricks job run.
        workspace_client (WorkspaceClient): Initialized Databricks SDK client.

    Returns:
        str: Error message, cause, or state message from the run output.
    """
    try:
        run_output = workspace_client.jobs.get_run_output(run_id=run_id)

        # Try multiple fields for a meaningful error message
        if run_output.error:
            return run_output.error
        elif run_output.cause:
            return run_output.cause
        elif run_output.metadata and run_output.metadata.state and run_output.metadata.state.state_message:
            return run_output.metadata.state.state_message
        else:
            return "No error message found"
    except Exception as e:
        return f"Error fetching run output for run_id={run_id}: {str(e)}"

# COMMAND ----------

# DBTITLE 1,send_pipeline_start_notification
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import json
import requests

def send_pipeline_start_notification(job_details_df, webhook_url, usecase, env, workspace_name):
    # Get current UTC and UAE times
    utc_now = datetime.now(timezone.utc)
    uae_now = utc_now.astimezone(ZoneInfo("Asia/Dubai"))

    # Format times
    notification_time_utc = utc_now.strftime("%m/%d/%Y %H:%M:%S")
    notification_time_uae = uae_now.strftime("%m/%d/%Y %H:%M:%S")

    # Get first row from DataFrame
    row = job_details_df.first()

    job_name = row['job_name']
    job_run_id = row['job_run_id']
    job_run_date = row['job_run_date']
    life_cycle_state = row['life_cycle_state']
    start_time = row['start_time']
    run_page_url = row['run_page_url']

    # Handle start_time
    formatted_start_time_utc = "N/A"
    formatted_start_time_uae = "N/A"
    if start_time:
        # If start_time is already UAE-adjusted, just display it directly
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=ZoneInfo("Asia/Dubai"))

        # Show UAE time directly
        formatted_start_time_uae = start_time.strftime("%m/%d/%Y %H:%M:%S")

        # Derive UTC by subtracting 4h
        formatted_start_time_utc = (start_time - timedelta(hours=4)).strftime("%m/%d/%Y %H:%M:%S")

    # Create the Teams message payload
    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": f"{job_name} pipeline execution started",
        "themeColor": "28a745",
        "title": f"🚀 ETL Pipeline Execution Started - {usecase.upper()} | {env.upper()}",
        "sections": [{
            "facts": [
                {"name": "Workspace:", "value": workspace_name},
                {"name": "Job Name:", "value": job_name},
                {"name": "Job Run Id:", "value": str(job_run_id)},
                {"name": "Status:", "value": f"⏳ {life_cycle_state.split('.')[-1]}"},
                {"name": "Start Time (UTC):", "value": formatted_start_time_utc},
                {"name": "Start Time (UAE):", "value": formatted_start_time_uae},
                {"name": "Message:", "value": f"Pipeline {job_name} execution started, indicating that all required source tables have been ingested successfully."},
                {"name": "Notification Time (UTC):", "value": notification_time_utc},
                {"name": "Notification Time (UAE):", "value": notification_time_uae},
            ],
            "markdown": True,
        }],
        "potentialAction": [{
            "@type": "OpenUri",
            "name": "View pipeline run",
            "targets": [{"os": "default", "uri": run_page_url}]
        }]
    }

    # Send the message
    response = requests.post(webhook_url, headers={"Content-Type": "application/json"}, data=json.dumps(message))

    if response.status_code == 200:
        print("Notification sent successfully.")
    else:
        print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")


# COMMAND ----------

# DBTITLE 1,send_pipeline_failure_notification
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import json
import requests

def send_pipeline_failure_notification(job_name, job_run_id, start_time, run_page_url, failed_task_run_id, error_details, webhook_url, usecase, env, workspace_name):
    # Get current time in UTC
    utc_now = datetime.now(timezone.utc)

    # Convert to UAE time
    uae_time = utc_now.astimezone(ZoneInfo("Asia/Dubai"))

    # Format as string
    notification_time = uae_time.strftime("%m/%d/%Y %H:%M:%S")

    from_zone = ZoneInfo("Asia/Dubai")
    to_zone = timezone.utc

    if start_time:
        if isinstance(start_time, (int, float)):
            # epoch milliseconds, just display as UAE
            start_time = datetime.fromtimestamp(start_time / 1000)

        elif isinstance(start_time, datetime):
            # if it's a datetime, assume it’s already UAE and just format
            if start_time.tzinfo:
                # drop tzinfo to avoid double-shifting
                start_time = start_time.replace(tzinfo=None)

        # Do NOT reconvert — just format
        start_time_uae = start_time.strftime("%m/%d/%Y %H:%M:%S")
    else:
        start_time_uae = "N/A"


    # Create the Teams message payload
    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": f"{job_name} pipeline failed",
        "themeColor": "FF0000",
        "title": f"🚀 ETL Pipeline Failed - {usecase.upper()} | {env.upper()}",
        "sections": [{
            "facts": [
                {"name": "Workspace:", "value": workspace_name},
                {"name": "Job Name:", "value": job_name},
                {"name": "Job Run Id:", "value": str(job_run_id)},
                {"name": "Status:", "value": f"❌ Failed"},
                {"name": "Failed Task Run Id:", "value": str(failed_task_run_id)},
                {"name": "Error Details:", "value": error_details},
                {"name": "Start Time (UAE):", "value": start_time_uae},
                {"name": "Message:", "value": f"Pipeline {job_name} has failed during execution. Investigate failed tasks and logs for further details."},
                {"name": "Notification Time (UAE):", "value": notification_time},
            ],
            "markdown": True,
        }],
        "potentialAction": [{
            "@type": "OpenUri",
            "name": "View pipeline run",
            "targets": [{"os": "default", "uri": run_page_url}]
        }]
    }

    # Send the message
    response = requests.post(webhook_url, headers={"Content-Type": "application/json"}, data=json.dumps(message))

    # Check response
    if response.status_code == 200:
        print("Notification sent successfully.")
        raise Exception("Failing the task intentionally after sending notification")
    else:
        print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")

# COMMAND ----------

# DBTITLE 1,send_pipeline_status_notification
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import json
import requests
import pandas as pd
from pyspark.sql.functions import col, max as spark_max, row_number, upper
from pyspark.sql.window import Window
 
def send_pipeline_status_notification(job_details_df, webhook_url, usecase, env, workspace_name):
    # Get current time in UTC
    utc_now = datetime.now(timezone.utc)
    # Convert to UAE time (UTC+4)
    uae_time = utc_now + timedelta(hours=4)
    # Format as string
    notification_time = uae_time.strftime("%m/%d/%Y %H:%M:%S")
 
    # Get first row from DataFrame
    row = job_details_df.first()
 
    # Parse values
    job_name = row['job_name']
    job_run_id = row['job_run_id']
    job_run_date = row['job_run_date']
    start_time = row['start_time']
    run_page_url = row['run_page_url']
    tasks_details = row['tasks_details']
 
    # print(tasks_details)
 
    from_zone = ZoneInfo("Asia/Dubai")
    to_zone = timezone.utc
 
    if start_time:
        if isinstance(start_time, (int, float)):
            start_time = datetime.fromtimestamp(start_time / 1000)
 
        elif isinstance(start_time, datetime):
            # If tz-aware, just drop tzinfo to avoid re-shifting
            if start_time.tzinfo:
                start_time = start_time.replace(tzinfo=None)
 
        # Don’t reconvert, just format
        formatted_start_time = start_time.strftime("%m/%d/%Y %H:%M:%S")
    else:
        formatted_start_time = "N/A"
 
 
    # Extract task_key and result_state safely
    task_status_list = []
    total_duration_ms = 0
 
    for task in tasks_details:
        try:
            task_key = task['task_key']
            # print(task_key)
            execution_duration = int(task['execution_duration'])
            # print(execution_duration)
            if 'result_state' in task['state'] and task['state']['result_state'] is not None:
                result_state = task['state']['result_state'].split('.')[-1]
            else:
                result_state = 'UNKNOWN'
 
            start_time = task['start_time']
 
            total_duration_ms += execution_duration  # Add to total
 
            task_status_list.append({
                "task_key": task_key,
                "result_state": result_state,
                "start_time": start_time,
                "execution_duration_ms": execution_duration
            })
 
        except Exception as e:
            print(f"Skipping task due to error: {e}")
 
    # Convert to pandas DataFrame
    task_status_df = spark.createDataFrame(task_status_list)
 
    # Cast start_time to timestamp type if it's not already
    task_status_df = task_status_df.withColumn("start_time", col("start_time").cast("bigint"))
 
    # Define window partitioned by task_key and ordered by start_time descending
    window_spec = Window.partitionBy("task_key").orderBy(col("start_time").desc())
 
    # Add row_number to select the latest start_time per task_key
    task_status_df_with_rank = task_status_df.withColumn("row_num", row_number().over(window_spec))
 
    # Filter only the latest row per task_key
    latest_start_df = task_status_df_with_rank.filter(col("row_num") == 1).drop("row_num")
 
    # Display the result
    # display(latest_start_df)
 
    latest_start_df = latest_start_df.withColumn(
    "execution_duration_ms",
    col("execution_duration_ms").cast("bigint")
    )
   
    total_duration_ms = latest_start_df.agg({"execution_duration_ms": "sum"}).collect()[0][0] or 0
    total_seconds = total_duration_ms // 1000
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    total_duration_hms = f"{hours}h {minutes}m {seconds}s"
 
    # Print total duration
    # print("🕒 Total Execution Duration:", total_duration_hms)
 
    failed_tasks_df = latest_start_df.filter(
        ~upper(col("result_state")).isin("SUCCESS", "RUNNING", "UNKNOWN","EXCLUDED")
    )
 
    # Determine if any failed task's task_key contains 'fact'
    fact_failed = failed_tasks_df.filter(col("task_key").rlike("(?i)fact")).count() > 0
 
    # Assign final status
    if failed_tasks_df.count() == 0:
        final_status = "SUCCESS"
    elif fact_failed:
        final_status = "FAILED"
    else:
        final_status = "SUCCESS_WITH_FAILURES"
 
 
    # print("📝 Final Status:", final_status)
 
    theme_colors = {
    "SUCCESS": "28a745",               # Green
    "SUCCESS_WITH_FAILURES": "ffa500", # Orange
    "FAILED": "ff0000"                 # Red
    }
 
    status_symbols = {
    "SUCCESS": "✅",
    "FAILED": "❌",
    "SUCCESS_WITH_FAILURES": "⚠️"
    }
 
    # Get symbol based on final_status
    status_symbol = status_symbols.get(final_status, "❓")  # Default to ❓ if unknown
 
    # print(f"{status_symbol} Final Status: {final_status}")
 
    # Create the Teams message payload
    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": f"{job_name} pipeline execution completed",
        "themeColor": theme_colors.get(final_status, "808080"),  # Default gray if unknown
        "title": f"🚀 ETL Pipeline Execution Completed - [{final_status}] - {usecase.upper()} | {env.upper()}",
        "sections": [{
            "facts": [
                {"name": "Workspace:", "value": workspace_name},
                {"name": "Job Name:", "value": job_name},
                {"name": "Job Run Id:", "value": str(job_run_id)},
                {"name": "Status:", "value": f"{status_symbol} {final_status}"},
                {"name": "Execution Start Time (UAE):", "value": formatted_start_time},
                {"name": "Execution Duration:", "value": total_duration_hms},
                {"name": "Message:", "value": f"Pipeline '{job_name}' execution completed with status: {final_status}."},
                {"name": "Notification Time (UAE):", "value": notification_time},
            ],
            "markdown": True,
        }],
        "potentialAction": [{
            "@type": "OpenUri",
            "name": "View pipeline run",
            "targets": [{"os": "default", "uri": run_page_url}]
        }]
    }
 
    # Send the message
    response = requests.post(webhook_url, headers={"Content-Type": "application/json"}, data=json.dumps(message))
 
    # Check response
    if response.status_code == 200:
        print("Notification sent successfully.")
    else:
        print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")
