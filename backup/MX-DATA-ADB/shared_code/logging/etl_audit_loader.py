# Databricks notebook source
# =============================================================================
# Notebook  : etl_audit_logs_all
# Purpose   : Ingest ADF pipeline & activity diagnostic logs from ADLS via
#             Auto Loader (cloudFiles), then create a unified audit view that
#             merges ADF and Databricks job run history.
#
# Hierarchy support
# -----------------
#   This notebook handles ADF pipelines with up to THREE levels of nesting:
#
#       Master pipeline
#         └─ Child pipeline  (e.g. pl_oracle_fusion_bicc_load)
#              └─ Grandchild pipeline  (e.g. pl_trigger_adb_ingestion)
#
#   Key signals (copy activity, set_total_rows, incremental_stats) may live
#   at ANY level.  The SQL resolves the full tree and always surfaces one
#   summary row per master pipeline run.
#
# Parameters
# ----------
#   env          : Deployment environment (dev / uat / prod).  Drives catalog
#                  names and storage account names.
#   subscription : Azure subscription GUID used to build the ADLS resource path.
#
# Output
# ------
#   Tables : transformation_{env}.etl_audit.adf_pipeline_runs
#            transformation_{env}.etl_audit.adf_activity_runs
#   View   : transformation_{env}.etl_audit.etl_audit_logs_all
# =============================================================================

# COMMAND ----------

# ---------------------------------------------------------------------------
# Widget declarations
# ---------------------------------------------------------------------------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("subscription", "")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Resolve widget values and set the active catalog
# ---------------------------------------------------------------------------
environment  = dbutils.widgets.get("env")
subscription = dbutils.widgets.get("subscription")

spark.sql(f"USE CATALOG transformation_{environment}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS etl_audit")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Helper : build ADLS Gen2 abfss paths for a given diagnostic-log container
#
# Returns
# -------
# path            : abfss URI pointing at ADF diagnostic logs
# checkpoint_path : abfss URI used by Auto Loader for offset checkpointing
# ---------------------------------------------------------------------------
def create_paths(container: str, environment: str) -> tuple:
    upper_env = environment.upper()

    path = (
        f"abfss://{container}"
        f"@stmxdatadatalake{environment}01.dfs.core.windows.net"
        f"/resourceId=/SUBSCRIPTIONS/{subscription}"
        f"/RESOURCEGROUPS/RG-MX-DATA-ADF-INGES-{upper_env}"
        f"/PROVIDERS/MICROSOFT.DATAFACTORY/FACTORIES"
        f"/ADF-MX-DATA-ADF-INGES-{upper_env}-01/"
    )

    checkpoint_path = (
        f"abfss://{container}"
        f"@stmxdatadatalake{environment}01.dfs.core.windows.net/_checkpoints/"
    )

    return path, checkpoint_path

# COMMAND ----------

# ---------------------------------------------------------------------------
# Ingest ADF pipeline-run diagnostic logs → etl_audit.adf_pipeline_runs
#
# Auto Loader reads only new JSON files since the last checkpoint, infers the
# schema automatically, and merges schema changes on the fly.
# ---------------------------------------------------------------------------
pipelineruns_container = "insights-logs-pipelineruns"
path, checkpoint_path  = create_paths(pipelineruns_container, environment)

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",         "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("cloudFiles.partitionColumns", "")
    .option("pathGlobFilter",            "*.json")
    .option("cloudFiles.inferSchema",    "true")
    .option("cloudFiles.allowOverwrites","true")
    .load(path)
    .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)          # process all backlog, then stop
    .option("mergeSchema", "true")
    .toTable(f"etl_audit.adf_pipeline_runs")
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Ingest ADF activity-run diagnostic logs → etl_audit.adf_activity_runs
# ---------------------------------------------------------------------------
activity_container    = "insights-logs-activityruns"
path, checkpoint_path = create_paths(activity_container, environment)

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",          "json")
    .option("cloudFiles.schemaLocation",  checkpoint_path)
    .option("cloudFiles.partitionColumns","")
    .option("pathGlobFilter",             "*.json")
    .option("cloudFiles.inferSchema",     "true")
    .option("cloudFiles.allowOverwrites", "true")
    .load(path)
    .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .option("mergeSchema", "true")
    .toTable(f"etl_audit.adf_activity_runs")
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Unified audit view : etl_audit.etl_audit_logs_all
#
# SQL structure (CTEs in order)
# ──────────────────────────────
#   1.  adf_pipeline_filtered    Base layer – deduplicate & keep terminal runs
#   2.  adf_all_pipelines        3-level tree resolution (master→child→grandchild)
#   3.  adf_copy_activity        Copy / DataFlow metrics across all tree levels
#   4.  adf_total_rows           set_total_rows signal across all tree levels
#   5.  adf_incremental_stats    set_ingest_incremental_stats across all levels
#   6.  adf_source_rows          Source row counts across all tree levels
#   7.  adf_combined             One summary row per master pipeline run (ADF)
#   8.  adb_jobs                 Databricks job run history (transformation +
#                                ingestion catalogs). Left-joined with
#                                adb_table_stats to fan out to run + table grain,
#                                consistent with the ADF side of the view.
#   9.  combined                 Final UNION ALL + ORDER BY
# ---------------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE VIEW etl_audit.etl_audit_logs_all AS
(

-- ============================================================================
-- CTE 1 : adf_pipeline_filtered
-- ----------------------------------------------------------------------------
-- Deduplicate the raw pipeline-run log table and retain only rows that
-- represent a terminal operation state (Succeeded or Failed).
-- Extracts typed timestamps, a human-readable duration string, and annotation
-- fields that carry business metadata (data_source, table_name, use_case).
-- ============================================================================
WITH adf_pipeline_filtered AS (
    SELECT DISTINCT
        runId,
        pipelineName,

        -- Cast ISO-8601 strings to proper timestamps
        CAST(`start` AS TIMESTAMP)                                              AS start_time,
        CAST(`end`   AS TIMESTAMP)                                              AS end_time,

        -- Human-readable duration: e.g. "1h 23m 45s"
        CONCAT(
            FLOOR(
                (unix_timestamp(CAST(`end` AS TIMESTAMP))
                 - unix_timestamp(CAST(`start` AS TIMESTAMP))) / 3600
            ), 'h ',
            FLOOR(
                MOD(
                    (unix_timestamp(CAST(`end` AS TIMESTAMP))
                     - unix_timestamp(CAST(`start` AS TIMESTAMP))),
                    3600
                ) / 60
            ), 'm ',
            MOD(
                (unix_timestamp(CAST(`end` AS TIMESTAMP))
                 - unix_timestamp(CAST(`start` AS TIMESTAMP))),
                60
            ), 's'
        )                                                                       AS execution_duration_hms,

        -- Raw duration in seconds for numeric aggregations
        (unix_timestamp(CAST(`end` AS TIMESTAMP))
         - unix_timestamp(CAST(`start` AS TIMESTAMP)))                         AS execution_duration,

        status,

        -- Business metadata stored in ADF pipeline annotations
        get_json_object(properties, '$.Annotations[0]')                        AS data_source,
        get_json_object(properties, '$.Annotations[1]')                        AS table_name,
        get_json_object(properties, '$.Annotations[2]')                        AS use_case,

        -- Pipeline parameters JSON blob (kept for audit traceability)
        get_json_object(properties, '$.Parameters')                            AS parameters,

        -- Parent linkage – used to reconstruct the pipeline hierarchy
        get_json_object(properties, '$.Predecessors[0].PipelineRunId')         AS parent_pipeline_runid,
        get_json_object(properties, '$.Predecessors[0].PipelineName')          AS parent_pipeline_name,

        tags

    FROM transformation_{environment}.etl_audit.adf_pipeline_runs

    -- Keep only terminal operation states; skip intermediate / in-progress
    WHERE operationName ILIKE '%Succeeded'
       OR operationName ILIKE '%Failed'
),


-- ============================================================================
-- CTE 2 : adf_all_pipelines
-- ----------------------------------------------------------------------------
-- Resolve the full 3-level pipeline hierarchy by self-joining
-- adf_pipeline_filtered twice:
--
--     child       (level 3 / grandchild  — the innermost pipeline)
--     parent      (level 2 / child)
--     grandparent (level 1 / master / root)
--
-- root_run_id always points to the topmost orchestrator regardless of how
-- many levels deep the current row sits.
--
-- Root-level metadata (status, timestamps, annotations) is pre-coalesced
-- here so downstream CTEs need only join on root_run_id.
-- ============================================================================
adf_all_pipelines AS (
    SELECT
        -- ── Current node identity ──────────────────────────────────────────
        child.runId,
        child.pipelineName,
        child.start_time,
        child.end_time,
        child.execution_duration,
        child.execution_duration_hms,
        child.status,
        child.data_source,
        child.table_name,
        child.use_case,
        child.parameters,
        child.tags,
        child.parent_pipeline_runid,
        child.parent_pipeline_name,

        -- ── Root (master) run identity ────────────────────────────────────
        -- Grandparent wins if present; otherwise parent; otherwise self
        COALESCE(grandparent.runId,        parent.runId,        child.runId)        AS root_run_id,
        COALESCE(grandparent.pipelineName, parent.pipelineName, child.pipelineName) AS master_pipeline_name,

        -- ── Root-level business metadata (coalesced for downstream use) ───
        COALESCE(grandparent.data_source,  parent.data_source,  child.data_source)  AS root_data_source,
        COALESCE(grandparent.table_name,   parent.table_name,   child.table_name)   AS root_table_name,
        COALESCE(grandparent.use_case,     parent.use_case,     child.use_case)     AS root_use_case,
        COALESCE(grandparent.parameters,   parent.parameters,   child.parameters)   AS root_parameters,
        COALESCE(grandparent.status,       parent.status,       child.status)       AS root_status,

        COALESCE(
            grandparent.execution_duration_hms,
            parent.execution_duration_hms,
            child.execution_duration_hms
        )                                                                            AS root_execution_duration_hms,

        COALESCE(
            grandparent.execution_duration,
            parent.execution_duration,
            child.execution_duration
        )                                                                            AS root_execution_duration,

        COALESCE(grandparent.start_time, parent.start_time, child.start_time)       AS root_start_time,
        COALESCE(grandparent.end_time,   parent.end_time,   child.end_time)         AS root_end_time,

        -- ── Intermediate node references (used for joins & depth detection) ─
        parent.runId                                                                 AS parent_run_id,
        parent.pipelineName                                                          AS parent_pipeline_name_resolved,
        grandparent.runId                                                            AS grandparent_run_id

    FROM      adf_pipeline_filtered child

    -- Level 2: direct parent of child
    LEFT JOIN adf_pipeline_filtered parent
           ON child.parent_pipeline_runid = parent.runId

    -- Level 1: grandparent (parent of the parent)
    LEFT JOIN adf_pipeline_filtered grandparent
           ON parent.parent_pipeline_runid = grandparent.runId
),


-- ============================================================================
-- CTE 3 : adf_copy_activity
-- ----------------------------------------------------------------------------
-- Extract Copy / ExecuteDataFlow activity metrics for each master pipeline run.
--
-- The join is made through adf_all_pipelines so the search covers every level
-- of the hierarchy — the copy activity may live in the child pipeline (2-level)
-- or in the grandchild pipeline (3-level).
--
-- Tiebreak within each root_run_id:
--   1. Prefer Copy over ExecuteDataFlow (more granular row counts)
--   2. Prefer the highest retry count (latest successful attempt)
-- ============================================================================
adf_copy_activity AS (
    SELECT * FROM (
        SELECT
            p.root_run_id,
            ar.activityType,

            -- Human-readable copy duration
            CONCAT(
                FLOOR(
                    (unix_timestamp(CAST(ar.`end` AS TIMESTAMP))
                     - unix_timestamp(CAST(ar.`start` AS TIMESTAMP))) / 3600
                ), 'h ',
                FLOOR(
                    MOD(
                        (unix_timestamp(CAST(ar.`end` AS TIMESTAMP))
                         - unix_timestamp(CAST(ar.`start` AS TIMESTAMP))),
                        3600
                    ) / 60
                ), 'm ',
                MOD(
                    (unix_timestamp(CAST(ar.`end` AS TIMESTAMP))
                     - unix_timestamp(CAST(ar.`start` AS TIMESTAMP))),
                    60
                ), 's'
            )                                                                   AS copy_activity_duration_hms,

            -- Raw copy duration in seconds
            (unix_timestamp(CAST(ar.`end` AS TIMESTAMP))
             - unix_timestamp(CAST(ar.`start` AS TIMESTAMP)))                  AS copy_activity_duration,

            -- Data volume metrics
            get_json_object(ar.properties, '$.Output.dataRead')                AS source_data_bytes,
            get_json_object(ar.properties, '$.Output.dataWritten')             AS data_written_bytes,

            -- Records read: extraction path varies by activity type
            CASE
                WHEN ar.activityType = 'Copy'
                    THEN get_json_object(ar.properties, '$.Output.rowsRead')
                WHEN ar.activityType = 'ExecuteDataFlow'
                    THEN get_json_object(
                             ar.properties,
                             '$.Output.runStatus.metrics.sink.sources.source.rowsRead'
                         )
                WHEN ar.activityName = 'set_total_rows'
                    THEN get_json_object(ar.properties, '$.Output.value')
            END                                                                 AS records_read,

            -- Records written / copied: extraction path varies by activity type
            CASE
                WHEN ar.activityType = 'Copy'
                    THEN get_json_object(ar.properties, '$.Output.rowsCopied')
                WHEN ar.activityType = 'ExecuteDataFlow'
                    THEN get_json_object(
                             ar.properties,
                             '$.Output.runStatus.metrics.sink.rowsWritten'
                         )
                WHEN ar.activityName = 'set_total_rows'
                    THEN get_json_object(ar.properties, '$.Output.value')
            END                                                                 AS records_copied,

            -- Error payload; non-null only on Failed runs
            get_json_object(ar.properties, '$.Error')                          AS Error,

            -- Rank to select one best-candidate row per root run
            ROW_NUMBER() OVER (
                PARTITION BY p.root_run_id
                ORDER BY
                    CASE ar.activityType WHEN 'Copy' THEN 0 ELSE 1 END,  -- prefer Copy
                    ar.activityRetryCount DESC                             -- prefer latest retry
            )                                                                   AS rn

        FROM transformation_{environment}.etl_audit.adf_activity_runs ar

        -- Join through the hierarchy map to find the copy activity at any level
        JOIN adf_all_pipelines p
          ON ar.pipelineRunId = p.runId

        WHERE ar.activityType IN ('Copy', 'ExecuteDataFlow')
          AND (ar.operationName ILIKE '%Succeeded' OR ar.operationName ILIKE '%Failed')
          AND ar.operationName NOT ILIKE 'update_watermark%'   -- exclude watermark copy activities
    )
    WHERE rn = 1   -- one best candidate per master run
),


-- ============================================================================
-- CTE 4 : adf_total_rows
-- ----------------------------------------------------------------------------
-- Capture the set_total_rows SetVariable output.
-- In the 3-level case this activity lives in the grandchild pipeline, so the
-- join through adf_all_pipelines is required to find it at any depth.
-- ============================================================================
adf_total_rows AS (
    SELECT * FROM (
        SELECT
            p.root_run_id,
            get_json_object(ar.properties, '$.Output.value')   AS total_rows_value,
            ROW_NUMBER() OVER (
                PARTITION BY p.root_run_id
                ORDER BY ar.activityRetryCount DESC
            )                                                   AS rn

        FROM transformation_{environment}.etl_audit.adf_activity_runs ar

        JOIN adf_all_pipelines p
          ON ar.pipelineRunId = p.runId

        WHERE ar.activityName = 'set_total_rows'
          AND (ar.operationName ILIKE '%Succeeded' OR ar.operationName ILIKE '%Failed')
    )
    WHERE rn = 1
),


-- ============================================================================
-- CTE 5 : adf_incremental_stats
-- ----------------------------------------------------------------------------
-- Capture the set_ingest_incremental_stats SetVariable output.
-- In the 3-level case this may live in the grandchild pipeline. The cross-level
-- join ensures it is found regardless of nesting depth.
-- ============================================================================
adf_incremental_stats AS (
    SELECT * FROM (
        SELECT
            p.root_run_id,
            get_json_object(ar.properties, '$.Output.value')   AS ingest_incremental_stats,
            ROW_NUMBER() OVER (
                PARTITION BY p.root_run_id
                ORDER BY ar.activityRetryCount DESC
            )                                                   AS rn

        FROM transformation_{environment}.etl_audit.adf_activity_runs ar

        JOIN adf_all_pipelines p
          ON ar.pipelineRunId = p.runId

        WHERE ar.activityName  = 'set_ingest_incremental_stats'
          AND ar.activityType  = 'SetVariable'
          AND (ar.operationName ILIKE '%Succeeded' OR ar.operationName ILIKE '%Failed')
    )
    WHERE rn = 1
),


-- ============================================================================
-- CTE 6 : adf_source_rows
-- ----------------------------------------------------------------------------
-- Purpose : Resolve the source-system record count for each root run.
--
--           The extraction path depends on the load pattern encoded in the
--           pipeline name:
--             • fullload    → rowsRead from the Copy activity
--             • cdc         → aggregate count from a Lookup/count activity
--             • incremental → old watermark from LookupNewWaterMarkActivity
--
--           The join through adf_all_pipelines ensures the activity is found
--           regardless of which level it runs at.
-- ============================================================================
adf_source_rows AS (
    SELECT * FROM (
        SELECT
            p.root_run_id,
            CASE
                -- Full-load: row count comes directly from the Copy activity output
                WHEN p.pipelineName ILIKE '%fullload%'
                    THEN get_json_object(ar.properties, '$.Output.rowsRead')

                -- CDC / incremental: row count comes from a pre-copy count lookup
                WHEN p.pipelineName ILIKE '%cdc%'
                  OR p.pipelineName ILIKE '%incremental%'
                    THEN COALESCE(
                             get_json_object(ar.properties, '$.Output.value[0].total_rows'),
                             get_json_object(ar.properties, '$.Output.value[0].TOTAL_ROWS')
                         )
            END                                                                 AS source_records,

            ROW_NUMBER() OVER (
                PARTITION BY p.root_run_id
                ORDER BY ar.activityRetryCount DESC
            )                                                                   AS rn

        FROM transformation_{environment}.etl_audit.adf_activity_runs ar

        JOIN adf_all_pipelines p
          ON ar.pipelineRunId = p.runId

        WHERE (
            -- Full-load: use the Copy activity row count
            (p.pipelineName ILIKE '%fullload%'     AND ar.activityType = 'Copy')
            -- CDC: use the count/lookup activity
         OR (p.pipelineName ILIKE '%cdc%'          AND ar.activityName ILIKE '%count%')
            -- Incremental: use the watermark lookup
         OR (p.pipelineName ILIKE '%incremental%'  AND ar.activityName = 'LookupNewWaterMarkActivity')
        )
        AND ar.operationName ILIKE '%Succeeded'
    )
    WHERE rn = 1
),


-- ============================================================================
-- CTE 7 : adf_combined
-- ----------------------------------------------------------------------------
-- Produce exactly ONE summary row per master pipeline run.
--
-- Filter: runId = root_run_id retains only the master-level row from
-- adf_all_pipelines. Child and grandchild rows have root_run_id pointing
-- to their ancestor (runId ≠ root_run_id) so they are excluded naturally.
--
-- All activity signals are joined on root_run_id, ensuring they are always
-- correlated to the correct master run regardless of the depth at which
-- they occurred.
--
-- Timestamps are converted from UTC to Arabian Standard Time (GST / Asia/Dubai)
-- to align with the operational timezone of the ADF factory.
--
-- Grain: 1 row per pipeline run. Each ADF pipeline targets one table so this
-- is equivalent to 1 row per run per table, consistent with the Databricks side.
-- ============================================================================
adf_combined AS (
    SELECT
        -- ── Source tag ────────────────────────────────────────────────────
        'ADF'                                                                   AS source,

        -- ── Business identity ─────────────────────────────────────────────
        root.root_use_case                                                      AS use_case,
        root.master_pipeline_name                                               AS job_name,
        NULL                                                                    AS job_id,
        root.root_run_id                                                        AS job_run_id,

        -- ── Run date (Gulf Standard Time) ─────────────────────────────────
        CAST(
            from_utc_timestamp(root.root_start_time, 'Asia/Dubai') AS DATE
        )                                                                       AS job_run_date,

        -- ── State ─────────────────────────────────────────────────────────
        'RunLifeCycleState.TERMINATED'                                          AS life_cycle_state,
        root.root_status                                                        AS result_state,
        ca.Error                                                                AS state_message,

        from_utc_timestamp(root.root_start_time, 'Asia/Dubai')                 AS start_time,
        from_utc_timestamp(root.root_end_time,   'Asia/Dubai')                 AS end_time,

        -- ── Duration metrics ──────────────────────────────────────────────
        NULL                                                                    AS setup_duration,
        NULL                                                                    AS setup_duration_hms,
        root.root_execution_duration                                            AS execution_duration,
        root.root_execution_duration_hms                                        AS execution_duration_hms,
        ca.copy_activity_duration,
        ca.copy_activity_duration_hms,

        -- ── Data source metadata ──────────────────────────────────────────
        root.root_data_source                                                   AS data_source,
        root.root_table_name                                                    AS table_name,

        -- ── Volume metrics ────────────────────────────────────────────────
        ca.source_data_bytes,
        ca.data_written_bytes,
        sr.source_records,
        ca.records_read,
        ca.records_copied,

        -- ── Incremental / CDC stats ───────────────────────────────────────
        -- Populated for incremental pipelines or 3-level runs
        -- (grandparent_run_id IS NOT NULL indicates at least 3 levels of nesting)
        CASE
            WHEN root.master_pipeline_name ILIKE '%incremental%'
              OR root.grandparent_run_id IS NOT NULL
            THEN ist.ingest_incremental_stats
            ELSE NULL
        END                                                                     AS job_stats,

        -- ── Activity type (Copy vs DataFlow) ──────────────────────────────
        ca.activityType,

        -- ── Traceability ──────────────────────────────────────────────────
        NULL                                                                    AS run_page_url,
        root.root_parameters                                                    AS tasks_details,
        NULL                                                                    AS failed_task_run_id,
        NULL                                                                    AS log_timestamp

    FROM (
        -- Emit only master-level rows; child/grandchild rows are excluded
        -- because their runId ≠ root_run_id
        SELECT DISTINCT *
        FROM   adf_all_pipelines
        WHERE  runId = root_run_id
    ) root

    LEFT JOIN adf_copy_activity     ca  ON root.root_run_id = ca.root_run_id
    LEFT JOIN adf_source_rows       sr  ON root.root_run_id = sr.root_run_id
    LEFT JOIN adf_incremental_stats ist ON root.root_run_id = ist.root_run_id
),


-- ============================================================================
-- CTE 8 : adb_jobs
-- ----------------------------------------------------------------------------
-- Pull Databricks job run history from both the transformation and ingestion
-- catalogs and normalise the schema to match adf_combined.
--
-- Grain: 1 row per job run per table, achieved by left-joining adb_table_stats
-- on job_run_id. This matches the ADF side where each pipeline run targets
-- exactly one table. Job runs with no entries in adb_table_stats still appear
-- once with NULL table_name and job_stats (left join behaviour), ensuring no
-- run is silently dropped from the audit view.
--
-- Column transformations applied to both catalogs:
--   result_state  : strip the fully-qualified enum prefix so only the terminal
--                   label (SUCCESS, FAILED, …) is surfaced.
--   state_message : normalise empty strings to NULL.
--   table_name    : strip schema prefix ("schema.table" → "table").
--   job_stats     : serialise the details struct to JSON; treat empty /
--                   "null" strings as NULL.
-- ============================================================================
adb_jobs AS (

    -- ── Transformation catalog ────────────────────────────────────────────
    SELECT
        "DATABRICKS"                                                            AS source,
        COALESCE(jr.usecase, '')                                                AS use_case,
        jr.job_name,
        CAST(jr.job_id     AS STRING)                                           AS job_id,
        CAST(jr.job_run_id AS STRING)                                           AS job_run_id,
        jr.job_run_date,
        jr.life_cycle_state,

        -- Strip enum class prefix: "RunResultState.SUCCESS" → "SUCCESS"
        SPLIT(
            COALESCE(jr.result_state, ''), '\\\\.'
        )[SIZE(SPLIT(COALESCE(jr.result_state, ''), '\\\\.')) - 1]             AS result_state,

        -- Normalise empty state messages to NULL
        CASE
            WHEN TRIM(jr.state_message) = '' THEN NULL
            ELSE jr.state_message
        END                                                                     AS state_message,

        jr.start_time,
        jr.end_time,
        jr.setup_duration,
        jr.setup_duration_hms,
        jr.execution_duration,
        jr.execution_duration_hms,

        -- Columns not applicable to Databricks runs
        NULL                                                                    AS copy_activity_duration,
        NULL                                                                    AS copy_activity_duration_hms,
        NULL                                                                    AS data_source,

        -- Strip schema prefix from table name: "schema.table" → "table"
        SPLIT(
            COALESCE(ts.table_name, ''), '\\\\.'
        )[SIZE(SPLIT(COALESCE(ts.table_name, ''), '\\\\.')) - 1]               AS table_name,

        NULL                                                                    AS source_data_bytes,
        NULL                                                                    AS data_written_bytes,
        NULL                                                                    AS source_records,
        NULL                                                                    AS records_read,
        NULL                                                                    AS records_copied,

        -- Serialise details struct to JSON; treat empty / null as NULL
        CASE
            WHEN ts.details IS NULL                                            THEN NULL
            WHEN TRIM(LOWER(CAST(ts.details AS STRING))) IN ('', 'null')      THEN NULL
            ELSE to_json(ts.details)
        END                                                                     AS job_stats,

        NULL                                                                    AS activityType,
        jr.run_page_url,
        CAST(jr.tasks_details AS STRING)                                        AS tasks_details,
        jr.failed_task_run_id,
        jr.log_timestamp

    FROM transformation_{environment}.etl_audit.adb_job_runs jr

    -- Fan out to run + table grain by joining all matching rows from
    -- adb_table_stats. Runs with no table stats return one row with
    -- NULL table_name and job_stats via the left join.
    LEFT JOIN transformation_{environment}.etl_audit.adb_table_stats ts
           ON CAST(jr.job_run_id AS STRING) = CAST(ts.current_job_run_id AS STRING)

    UNION ALL

    -- ── Ingestion catalog (identical transformation logic) ────────────────
    SELECT
        "DATABRICKS"                                                            AS source,
        COALESCE(jr.usecase, '')                                                AS use_case,
        jr.job_name,
        CAST(jr.job_id     AS STRING)                                           AS job_id,
        CAST(jr.job_run_id AS STRING)                                           AS job_run_id,
        jr.job_run_date,
        jr.life_cycle_state,

        SPLIT(
            COALESCE(jr.result_state, ''), '\\\\.'
        )[SIZE(SPLIT(COALESCE(jr.result_state, ''), '\\\\.')) - 1]             AS result_state,

        CASE
            WHEN TRIM(jr.state_message) = '' THEN NULL
            ELSE jr.state_message
        END                                                                     AS state_message,

        jr.start_time,
        jr.end_time,
        jr.setup_duration,
        jr.setup_duration_hms,
        jr.execution_duration,
        jr.execution_duration_hms,
        NULL                                                                    AS copy_activity_duration,
        NULL                                                                    AS copy_activity_duration_hms,
        NULL                                                                    AS data_source,

        SPLIT(
            COALESCE(ts.table_name, ''), '\\\\.'
        )[SIZE(SPLIT(COALESCE(ts.table_name, ''), '\\\\.')) - 1]               AS table_name,

        NULL                                                                    AS source_data_bytes,
        NULL                                                                    AS data_written_bytes,
        NULL                                                                    AS source_records,
        NULL                                                                    AS records_read,
        NULL                                                                    AS records_copied,

        CASE
            WHEN ts.details IS NULL                                            THEN NULL
            WHEN TRIM(LOWER(CAST(ts.details AS STRING))) IN ('', 'null')      THEN NULL
            ELSE to_json(ts.details)
        END                                                                     AS job_stats,

        NULL                                                                    AS activityType,
        jr.run_page_url,
        CAST(jr.tasks_details AS STRING)                                        AS tasks_details,
        jr.failed_task_run_id,
        jr.log_timestamp

    FROM ingestion_{environment}.etl_audit.adb_job_runs jr

    LEFT JOIN ingestion_{environment}.etl_audit.adb_table_stats ts
           ON CAST(jr.job_run_id AS STRING) = CAST(ts.current_job_run_id AS STRING)
),


-- ============================================================================
-- CTE 9 : combined
-- ============================================================================
-- Union ADF and Databricks audit rows into a single result set.
-- UNION ALL is intentional: both sides produce distinct job run IDs so
-- duplicate elimination is unnecessary and would add cost.
-- ============================================================================
combined AS (
    SELECT * FROM adf_combined
    UNION ALL
    SELECT * FROM adb_jobs
)


-- ============================================================================
-- Final SELECT
-- ----------------------------------------------------------------------------
-- Order by start_time DESC so the most recent runs appear first in dashboards
-- and ad-hoc queries without an explicit ORDER BY.
-- ============================================================================
SELECT *
FROM   combined
ORDER BY start_time DESC

)
""")