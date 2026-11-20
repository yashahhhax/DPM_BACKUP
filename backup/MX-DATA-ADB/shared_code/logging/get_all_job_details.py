# Databricks notebook source

# =============================================================================
# Notebook  : get_all_job_details
# Purpose   : Fetches the latest run details for all jobs defined in
#             job_config and upserts them into the ETL audit Delta table
#             (adb_job_runs).
#
#             adb_job_runs is maintained at a strict 1-row-per-job-run grain.
#             Table-level metrics (table_name, details) live in adb_table_stats
#             and are joined at query time inside the etl_audit_logs_all view.
#
# Inputs    : job_config  – JSON widget defining master job and optional child
#                           jobs with their tags.
#
#             Expected schema:
#             {
#               "master_job": { "job_name": "...", "tag": "..." },
#               "job_names":  [ { "job_name": "...", "tag": "..." }, ... ]
#             }
#
# Output    : {catalog}.etl_audit.adb_job_runs  (Delta, upsert)
# =============================================================================

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("job_config", "", "Job Config (JSON)")

# COMMAND ----------

# DBTITLE 1,Import utility functions
# MAGIC %run ./logger_utility_functions

# COMMAND ----------

# DBTITLE 1,Read notebook parameters
job_config = dbutils.widgets.get("job_config").strip()

if not job_config:
    raise ValueError(
        "[ERROR] 'job_config' widget is empty or not provided. "
        "Expected a JSON string with 'master_job' and optionally 'job_names'."
    )

print(f"[INFO] job_config = {job_config}")

# COMMAND ----------

# DBTITLE 1,Resolve workspace and catalog
workspace_info = get_workspace_info(spark)
catalog        = workspace_info["catalog_name"]

print(f"[INFO] Resolved catalog: {catalog}")

# COMMAND ----------

# DBTITLE 1,Parse and validate job config
import json

try:
    config = json.loads(job_config)
except json.JSONDecodeError as e:
    raise ValueError(f"[ERROR] Invalid JSON in 'job_config': {str(e)}")

# master_job is mandatory — the primary job whose run is being audited
master_job_name = config["master_job"]["job_name"]
master_job_tag  = config["master_job"]["tag"]

# job_names is optional (single-job workflows may omit it).
# Master job is always first. Duplicates are safely ignored.
all_jobs_with_tags = [(master_job_name, master_job_tag)]

for job in config.get("job_names", []):
    entry = (job["job_name"], job["tag"])
    if entry not in all_jobs_with_tags:
        all_jobs_with_tags.append(entry)

print(f"[INFO] Master job : {(master_job_name, master_job_tag)}")
print(f"[INFO] All jobs   : {all_jobs_with_tags}")

# COMMAND ----------

# DBTITLE 1,Define table references
schema          = "etl_audit"
etl_audit_table = "adb_job_runs"
etl_log_table   = f"{catalog}.{schema}.{etl_audit_table}"

# Ensure the audit schema exists before any read/write operations
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"[INFO] Target audit table: {etl_log_table}")

# COMMAND ----------

# DBTITLE 1,Fetch latest run IDs for all jobs
# Calls the Databricks Jobs API to retrieve the most recent run ID
# for each (job_name, tag) pair.
# Returns a DataFrame with columns: job_name, tag, job_id, latest_run_id
jobs_df = get_latest_run_ids_for_job_names(all_jobs_with_tags, spark)

if jobs_df.isEmpty():
    print("[WARN] No run IDs found for the provided jobs. Verify job names and tags in job_config.")
    dbutils.notebook.exit("SKIPPED: No run IDs found. Nothing to log.")

display(jobs_df)

# COMMAND ----------

# DBTITLE 1,Fetch full job run details
# Calls the Databricks Jobs API for each run ID to retrieve run-level
# metadata: status, lifecycle state, start/end times, duration,
# task details, and error messages for failed runs.
# Returns None if all API calls fail.
run_log_df = get_job_run_details(jobs_df, spark)

if run_log_df is None:
    print("[WARN] No run details could be fetched. All API calls may have failed.")
    dbutils.notebook.exit("SKIPPED: No run details returned. Nothing to log.")

# Cast job_run_id to STRING to match the target table schema
run_log_df = run_log_df.withColumn("job_run_id", col("job_run_id").cast("string"))

display(run_log_df)

# COMMAND ----------

# DBTITLE 1,Upsert run details into audit Delta table
from delta.tables import DeltaTable

if spark.catalog.tableExists(etl_log_table):

    # Merge on the natural key: job_name + job_id + job_run_id.
    # Matched rows are updated to capture status transitions
    # (e.g. RUNNING → SUCCESS) when a run completes after the
    # first poll. Unmatched rows are inserted as new audit records.
    (
        DeltaTable.forName(spark, etl_log_table)
        .alias("target")
        .merge(
            run_log_df.alias("source"),
            """
            target.job_name   = source.job_name  AND
            target.job_id     = source.job_id    AND
            target.job_run_id = source.job_run_id
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"[INFO] Merge completed into {etl_log_table}.")

else:
    # First run: create the table from the DataFrame schema
    (
        run_log_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(etl_log_table)
    )

    print(f"[INFO] Table {etl_log_table} created and initial data written.")