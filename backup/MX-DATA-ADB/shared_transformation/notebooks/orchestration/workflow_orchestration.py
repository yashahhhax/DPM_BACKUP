# Databricks notebook source
# =============================================================================
# Workflow Orchestration Trigger
# =============================================================================
# Purpose:
#   Scans config.workflow_dependencies for workflows whose source tables have
#   been refreshed since the workflow's last run, triggers those that are
#   "ready", and performs housekeeping by cancelling stray duplicate runs of
#   this orchestrator job itself.
#
# Expects:
#   - A widget "environment" (e.g. dev/staging/prod) used to select the
#     transformation catalog (transformation_<environment>). Only tags and
#     Delta history from that catalog are used for freshness.
#   - config.workflow_dependencies(use_case, workflow_name, table_name,
#     freshness_requirement_hours, hard_dependency, workflow_active_flag)
#     with table_name as "schema.table" in the transformation catalog.
#
# Freshness source (per table):
#   1. PRIMARY  - table tags 'last_updated' / 'last_failure' in
#                 system.information_schema.table_tags.
#   2. FALLBACK - DESCRIBE HISTORY, used ONLY for dependency tables that have
#                 no 'last_updated' tag. Tagged tables are never queried for
#                 history, so the expensive per-table calls are limited to
#                 the untagged ones.
#   The source used for each table is shown in the `freshness_source` column
#   of workflow_debug_status ('tag', 'history' or 'none').
#
# Idempotency:
#   Safe to re-run / re-trigger via the job scheduler. Workflows are only
#   triggered once their dependency freshness check passes, and this notebook
#   cancels its own duplicate concurrent runs at the end (keeping the 2 oldest
#   active ones) to guard against overlapping scheduler triggers.
# =============================================================================

dbutils.widgets.text("environment", "dev")

# COMMAND ----------

# Get environment from widget parameter
environment = dbutils.widgets.get("environment")

# Set catalog names based on environment
ingestion_catalog = "ingestion_" + environment
transformation_catalog = "transformation_" + environment
spark.sql(f"use catalog {transformation_catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET timezone=Asia/Dubai

# COMMAND ----------

# =============================================================================
# Imports and configuration
# =============================================================================
from databricks.sdk import WorkspaceClient
from datetime import datetime
from typing import Dict, Optional, List, Any, Set
from concurrent.futures import as_completed, ThreadPoolExecutor
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import re

def short_error(e: Exception) -> str:
    """
    Collapse an exception down to its first line.

    PySpark/JVM exceptions (e.g. AnalysisException for a missing table) embed
    a full Java stack trace inside str(e) after a "JVM stacktrace" marker --
    useful in a debugger, unreadable as a print() line. This keeps the actual
    error reason (e.g. "[TABLE_OR_VIEW_NOT_FOUND] ...") and drops everything
    from the stack trace onward.
    """
    message = re.split(r"JVM [Ss]tacktrace", str(e), maxsplit=1)[0].strip()
    return message if message else type(e).__name__

# Explicit schemas for the DataFrames we build from Python lists. Without
# these, spark.createDataFrame() infers schema from sampled rows and RAISES
# if the list happens to be empty (e.g. every dependency table is tagged so
# no history is fetched, or no workflows are configured yet).
TABLE_METADATA_SCHEMA = StructType([
    StructField("schema_name", StringType(), nullable=False),
    StructField("table_name", StringType(), nullable=False),
    StructField("last_updated", TimestampType(), nullable=True),
    StructField("action_performed", StringType(), nullable=True)
])

WORKFLOW_LAST_RUN_SCHEMA = StructType([
    StructField("workflow_name", StringType(), nullable=False),
    StructField("workflow_last_start_time", TimestampType(), nullable=True)
])

# COMMAND ----------

# =============================================================================
# Databricks Jobs API helpers
# =============================================================================
def get_workspace_client() -> WorkspaceClient:
    """Create a Databricks workspace client. Call once per notebook run and
    pass the instance around -- creating a fresh client per call is wasteful
    and was the pattern in the original version of this notebook."""
    return WorkspaceClient()

def build_job_cache(workspace_client: WorkspaceClient) -> Dict[str, Any]:
    """
    List all jobs ONCE and index them by name.

    Rationale: every lookup site below (trigger_job, get_last_runs,
    cancel_runs_after_first_two) used to call workspace_client.jobs.list()
    independently to find a single job by name. That's an O(workflows) number
    of full workspace job listings per notebook run. Building the cache once
    up front turns every subsequent lookup into an O(1) dict access.

    Note on duplicate job names: Databricks does not enforce unique job names.
    If two jobs share a name, the FIRST one encountered in the API's listing
    order wins here -- a plain {job.settings.name: job for job in ...} dict
    comprehension would instead let the LAST duplicate silently win, which
    would be a correctness regression for any workspace with duplicate names.
    """
    job_cache: Dict[str, Any] = {}
    for job in workspace_client.jobs.list():
        if job.settings and job.settings.name and job.settings.name not in job_cache:
            job_cache[job.settings.name] = job
    return job_cache

def find_job(job_cache: Dict[str, Any], job_name: str) -> Optional[Any]:
    """Look up a job by name from the prebuilt cache (no API call)."""
    return job_cache.get(job_name)

def get_job_details(job_cache: Dict[str, Any], job_name: str) -> Dict:
    """
    Get details of a Databricks job by its name.

    Args:
        job_cache: Cache built once via build_job_cache().
        job_name: Name of the Databricks job.

    Returns:
        Dict with job_id and job_name.

    Raises:
        ValueError: If no job is found with the given name.
    """
    job = find_job(job_cache, job_name)
    if not job:
        raise ValueError(f"No job found with name: {job_name}")

    return {
        "job_id": job.job_id,
        "job_name": job.settings.name
    }

def trigger_job(workspace_client: WorkspaceClient, job_cache: Dict[str, Any], job_name: str) -> Dict:
    """
    Trigger a Databricks job by name via Jobs API run-now.

    Args:
        workspace_client: Shared SDK client.
        job_cache: Cache built once via build_job_cache().
        job_name: Name of the Databricks job to trigger.

    Returns:
        Dict with the raw run_response, run_id, and a status marker.

    Raises:
        ValueError: Propagated from get_job_details if job_name is unknown.
        Exception: Any error raised by the Jobs API run_now call itself.
    """
    job = get_job_details(job_cache, job_name)
    run_response = workspace_client.jobs.run_now(job_id=job["job_id"])

    return {
        "run_response": run_response,
        "run_id": run_response.run_id,
        "status": "RUNNING"
    }

def cancel_runs_after_first_two(workspace_client: WorkspaceClient, job_cache: Dict[str, Any], job_name: str) -> Dict:
    """
    Cancel all active (running/queued) runs for a Databricks job after the
    2 oldest ones.

    This is a housekeeping guard against overlapping scheduler triggers
    causing this orchestrator job to pile up duplicate concurrent runs.

    Args:
        workspace_client: Shared SDK client.
        job_cache: Cache built once via build_job_cache().
        job_name: Name of the Databricks job to clean up.

    Returns:
        Dict with job_name and the list of run IDs that were cancelled.
    """
    job = get_job_details(job_cache, job_name)
    runs = list(workspace_client.jobs.list_runs(job_id=job["job_id"], active_only=True))

    # Sort oldest-first. A queued run that hasn't started yet can have
    # start_time=None, so default it to 0 rather than letting sorted() raise
    # a TypeError on a None-vs-int comparison.
    runs_sorted = sorted(runs, key=lambda r: r.start_time or 0)

    cancelled_run_ids: List[Any] = []
    for run in runs_sorted[2:]:
        print(f"Cancelling stray run {run.run_id} for job {job_name}")
        workspace_client.jobs.cancel_run(run_id=run.run_id)
        cancelled_run_ids.append(run.run_id)

    return {"job_name": job_name, "cancelled_run_ids": cancelled_run_ids}

# COMMAND ----------

# =============================================================================
# Table freshness: tags first, history as fallback
# =============================================================================
def get_workflow_dependencies() -> List[str]:
    """Distinct list of tables referenced anywhere in config.workflow_dependencies."""
    rows = spark.sql("SELECT DISTINCT table_name FROM config.workflow_dependencies").collect()
    return [row["table_name"] for row in rows]

def get_tagged_tables(catalog: str) -> Set[str]:
    """
    Tables ("schema.table") in `catalog` that already carry a valid
    'last_updated' tag. These get their freshness from tags and skip history.

    Args:
        catalog: Catalog whose table_tags are read (the transformation catalog).
    """
    rows = spark.sql(f"""
        SELECT DISTINCT concat(schema_name, '.', table_name) AS table_name
        FROM system.information_schema.table_tags
        WHERE tag_name = 'last_updated'
          AND TRY_CAST(tag_value AS TIMESTAMP) IS NOT NULL
          AND catalog_name = '{catalog}'
    """).collect()
    return {row["table_name"] for row in rows}

def get_table_metadata(table: str) -> Optional[dict]:
    """
    Retrieve the most recent Delta history entry for a table.

    Used only as a fallback for dependency tables that have no valid
    'last_updated' tag.

    Args:
        table: "schema.table" name, resolved against the current
               (transformation) catalog.

    Returns:
        Dict with schema_name, table_name, last_updated, action_performed,
        or None if history could not be retrieved (e.g. not a Delta table,
        a view, a missing table, no history yet, or a transient catalog
        error).
    """
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()
        if not history:
            return None
        schema_name, table_name = table.split(".")
        return {
            "schema_name": schema_name,
            "table_name": table_name,
            "last_updated": history[0]["timestamp"],
            "action_performed": history[0]["operation"],
        }
    except Exception as e:
        print(f"History fallback failed for table {table}: {short_error(e)}")
        return None

def collect_table_metadata(tables: List[str]):
    """
    Collect last-updated metadata from Delta history for tables that have
    no 'last_updated' tag (the fallback path), in parallel.

    Tagged tables are never passed in, so DESCRIBE HISTORY only runs for
    the untagged ones. Tables whose history can't be read are logged and
    skipped; they end up with freshness_source = 'none'.

    Args:
        tables: List of untagged "schema.table" names.

    Returns:
        Spark DataFrame conforming to TABLE_METADATA_SCHEMA (possibly empty).
    """
    metadata: List[dict] = []
    if tables:
        with ThreadPoolExecutor(max_workers=16) as executor:
            futures = [executor.submit(get_table_metadata, table) for table in tables]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    metadata.append(result)

    # Explicit schema avoids createDataFrame's "can not infer schema from an
    # empty dataset" error when there are no untagged tables, or when
    # history couldn't be read for any of them.
    return spark.createDataFrame(metadata, schema=TABLE_METADATA_SCHEMA)

# COMMAND ----------

# =============================================================================
# Workflow last-run collection
# =============================================================================
def get_last_runs(workspace_client: WorkspaceClient, job_cache: Dict[str, Any]):
    """
    Get the most recent run start time for every workflow referenced in
    config.workflow_dependencies, using the shared job cache instead of
    re-listing jobs per workflow.

    Args:
        workspace_client: Shared SDK client.
        job_cache: Cache built once via build_job_cache().

    Returns:
        Spark DataFrame conforming to WORKFLOW_LAST_RUN_SCHEMA (possibly empty).
    """
    workflows = spark.sql("SELECT DISTINCT workflow_name FROM config.workflow_dependencies").collect()
    workflow_last_runs: List[dict] = []

    for workflow in workflows:
        workflow_name = workflow["workflow_name"]
        job = find_job(job_cache, workflow_name)

        if not job:
            print(f"No job found for workflow '{workflow_name}'")
            continue

        runs = workspace_client.jobs.list_runs(job_id=job.job_id, limit=1, completed_only=False)
        last_run = next(iter(runs), None)

        if last_run:
            last_start_time = datetime.fromtimestamp(last_run.start_time / 1000)
            print(f"Last run for workflow '{workflow_name}' started at {last_start_time}")
        else:
            last_start_time = datetime(1900, 1, 1)
            print(f"No runs found for workflow '{workflow_name}'. Using sentinel {last_start_time}.")

        workflow_last_runs.append({
            "workflow_name": workflow_name,
            "workflow_last_start_time": last_start_time
        })

    return spark.createDataFrame(workflow_last_runs, schema=WORKFLOW_LAST_RUN_SCHEMA)

# COMMAND ----------

# =============================================================================
# Workflow readiness evaluation
# =============================================================================
def get_ready_workflows(catalog: str) -> List[str]:
    """
    Determine which workflows are ready to trigger.

    A workflow is "ready" when every one of its dependent tables has been
    updated/executed since the workflow's own last start time (adjusted for
    any minimum freshness requirement in hours).

    Building blocks (materialized as temp view `workflow_debug_status`):
      - tag_updates: freshness from 'last_updated' / 'last_failure' tags
        (single scan, conditional aggregation).
      - history_updates: Delta history fallback (table_metadata), populated
        only for tables without a 'last_updated' tag.
      - tables_max_updates: merges the two per table.
          * table_last_updated  = tag 'last_updated', else history timestamp
          * table_last_executed = latest of tag 'last_updated',
                                  tag 'last_failure' and history timestamp
          * freshness_source    = 'tag' / 'history' / 'none'
      - workflow_dependencies: joins each workflow's table dependencies
        against its last known start time, applying the freshness window.
      - final SELECT: flags each dependency row as satisfied or not, with a
        carve-out for soft dependencies on the DRR workflows
        ('wf_master_drr_omni', 'wf_master_drr_clymb', 'wf_master_drr_ig'),
        which are auto-satisfied after their first run.

    Args:
        catalog: Catalog whose table_tags are read (the transformation catalog).

    Returns:
        List of workflow names where every dependency is satisfied.
    """
    debug_query = f"""
        WITH tag_updates AS (
            SELECT
                concat(schema_name, '.', table_name) AS table_name,
                MAX(CASE WHEN tag_name = 'last_updated' THEN TRY_CAST(tag_value AS TIMESTAMP) END) AS tag_last_updated,
                MAX(CASE WHEN tag_name IN ('last_updated', 'last_failure') THEN TRY_CAST(tag_value AS TIMESTAMP) END) AS tag_last_executed
            FROM system.information_schema.table_tags
            WHERE tag_name IN ('last_updated', 'last_failure')
              AND catalog_name = '{catalog}'
            GROUP BY concat(schema_name, '.', table_name)
        ),
        history_updates AS (
            SELECT
                concat(schema_name, '.', table_name) AS table_name,
                MAX(last_updated) AS history_last_updated
            FROM table_metadata
            GROUP BY concat(schema_name, '.', table_name)
        ),
        tables_max_updates AS (
            SELECT
                COALESCE(t.table_name, h.table_name) AS table_name,
                -- Tag wins; history only fills in when the tag is missing.
                COALESCE(t.tag_last_updated, h.history_last_updated) AS table_last_updated,
                -- GREATEST ignores NULLs, so this works with either source missing.
                GREATEST(t.tag_last_executed, h.history_last_updated) AS table_last_executed,
                CASE
                    WHEN t.tag_last_updated IS NOT NULL THEN 'tag'
                    WHEN h.history_last_updated IS NOT NULL THEN 'history'
                    ELSE 'none'
                END AS freshness_source
            FROM tag_updates t
            FULL OUTER JOIN history_updates h ON t.table_name = h.table_name
        ),
        workflow_dependencies AS (
            SELECT
                w.use_case,
                w.workflow_name,
                w.table_name,
                w.freshness_requirement_hours,
                w.hard_dependency,
                lst.workflow_last_start_time,
                CASE
                    WHEN w.freshness_requirement_hours > 0
                        THEN GREATEST(
                            DATEADD(HOUR, -w.freshness_requirement_hours, GETDATE()),
                            COALESCE(lst.workflow_last_start_time, '1900-01-01')
                        )
                    ELSE lst.workflow_last_start_time
                END AS adjusted_workflow_last_start_time
            FROM config.workflow_dependencies w
            LEFT JOIN last_start_times lst ON w.workflow_name = lst.workflow_name
            WHERE w.workflow_active_flag = true
        )
        SELECT
            wd.use_case,
            wd.workflow_name,
            wd.table_name,
            wd.workflow_last_start_time,
            wd.adjusted_workflow_last_start_time,
            wd.hard_dependency,
            tmu.table_last_updated,
            tmu.table_last_executed,
            COALESCE(tmu.freshness_source, 'none') AS freshness_source,
            wd.freshness_requirement_hours,
            CASE
                -- From the second run onward, auto-satisfy soft dependencies
                -- for the configured DRR workflows.
                WHEN wd.hard_dependency = false
                     AND wd.workflow_name IN ('wf_master_drr_omni', 'wf_master_drr_clymb', 'wf_master_drr_ig')
                     AND wd.workflow_last_start_time IS NOT NULL
                THEN 1
                -- Default: check freshness normally.
                ELSE CASE
                    WHEN (CASE WHEN wd.hard_dependency THEN tmu.table_last_updated ELSE tmu.table_last_executed END)
                         > wd.adjusted_workflow_last_start_time
                    THEN 1 ELSE 0
                END
            END AS updated_since_last_start
        FROM workflow_dependencies wd
        LEFT JOIN tables_max_updates tmu ON wd.table_name = tmu.table_name
    """
    debug_df = spark.sql(debug_query)
    debug_df.createOrReplaceTempView("workflow_debug_status")

    # A workflow is ready only when ALL of its active dependency rows are satisfied.
    final_query = """
        SELECT workflow_name
        FROM (
            SELECT
                workflow_name,
                COUNT(*) AS total_tables,
                SUM(updated_since_last_start) AS updated_since_last_start
            FROM workflow_debug_status
            GROUP BY workflow_name
        )
        WHERE updated_since_last_start = total_tables
    """
    ready_df = spark.sql(final_query)
    return [row.workflow_name for row in ready_df.collect()]

# COMMAND ----------

# =============================================================================
# Build shared client / job cache, then gather table and workflow metadata
# =============================================================================
workspace_client = get_workspace_client()
job_cache = build_job_cache(workspace_client)  # one jobs.list() call, reused below

table_dependencies = get_workflow_dependencies()
tagged_tables = get_tagged_tables(transformation_catalog)
untagged_tables = [t for t in table_dependencies if t not in tagged_tables]
print(
    f"Found {len(table_dependencies)} dependency table(s): "
    f"{len(table_dependencies) - len(untagged_tables)} with tags, "
    f"{len(untagged_tables)} falling back to history."
)

# History is only fetched for tables without a 'last_updated' tag.
table_metadata = collect_table_metadata(untagged_tables)
table_metadata.createOrReplaceTempView("table_metadata")

last_start_times = get_last_runs(workspace_client, job_cache)
last_start_times.createOrReplaceTempView("last_start_times")

# COMMAND ----------

# Determine which workflows are ready to run
ready_flow_names = get_ready_workflows(transformation_catalog)
print(f"{len(ready_flow_names)} workflow(s) ready to trigger: {ready_flow_names}")
display(ready_flow_names)

# COMMAND ----------

# =============================================================================
# Trigger ready workflows
# =============================================================================
# Triggered concurrently since get_ready_workflows() only returns workflows
# whose dependencies are independently satisfied -- there's no ordering
# requirement between them. Each trigger is isolated: one workflow failing to
# trigger (e.g. a transient API error, or a job deleted from the workspace
# after job_cache was built) is logged and does not block the others.
if not ready_flow_names:
    print("No workflows ready to trigger this run.")
else:
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_flow = {
            executor.submit(trigger_job, workspace_client, job_cache, flow_name): flow_name
            for flow_name in ready_flow_names
        }
        for future in as_completed(future_to_flow):
            flow_name = future_to_flow[future]
            try:
                result = future.result()
                print(f"Triggered workflow '{flow_name}' (run_id={result['run_id']})")
            except Exception as e:
                print(f"Failed to trigger workflow '{flow_name}': {short_error(e)}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workflow_debug_status

# COMMAND ----------

# =============================================================================
# Housekeeping: cancel stray duplicate runs of this orchestrator job itself
# =============================================================================
cancel_summary = cancel_runs_after_first_two(workspace_client, job_cache, "workflow_orchestration")
print(f"Cancellation summary: {cancel_summary}")