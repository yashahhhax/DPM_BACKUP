# Databricks notebook source
dbutils.widgets.text("usecase","")
dbutils.widgets.text("env","")
dbutils.widgets.text("module","")
dbutils.widgets.text("email_key","")
dbutils.widgets.text("receiver_email_id","")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("etl_job_alerts_required", "", "Etl Job Alerts Required")

# COMMAND ----------

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

# DBTITLE 1,serverless ansi off
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %run ../common_functions/base_functions

# COMMAND ----------

# MAGIC %run ./logger_utility_functions

# COMMAND ----------

from datetime import datetime

try:
    notebook_vars = ['env','usecase', 'module','secret_scope', 'email_key', 'receiver_email_id']
    
    # Unpack with defaults as None
    env, usecase, module, secret_scope, email_key,receiver_email_id = get_notebook_params(*notebook_vars, verbose=True)
    usecase=usecase.lower()

except Exception as e:
    print(f"[ERROR] {str(e)}")
    raise


# COMMAND ----------

run_logs_df = spark.table(f"transformation_{env}.etl_audit.etl_audit_logs_all")
pbi_refresh_logs_df = spark.table(f"transformation_{env}.etl_audit.pbi_refresh_history")

# COMMAND ----------

latest_pbi_refresh_logs_df = spark.sql(f"""
WITH dataset_filtered AS (
  SELECT dataset_id
  FROM transformation_{env}.config.pbi_datasets
  WHERE lower(module) = '{module}'
),
ranked_logs AS (
  SELECT
    r.dataset_id,
    r.refresh_date,
    r.report_name AS dataset_name,
    r.refresh_type,
    r.refresh_duration_hms,
    r.status,
    r.error_message,
    ROW_NUMBER() OVER (PARTITION BY r.dataset_id ORDER BY r.start_time DESC) AS rn
  FROM transformation_{env}.etl_audit.pbi_refresh_history r
  JOIN dataset_filtered d
    ON r.dataset_id = d.dataset_id
  WHERE lower(r.usecase) = '{usecase}'
    AND r.refresh_date = current_date()
)

SELECT *
FROM ranked_logs
WHERE rn = 1
"""
).drop("rn")


# COMMAND ----------

display(latest_pbi_refresh_logs_df)

# COMMAND ----------

# Construct dynamic WHERE filter

if module:
    filter_clause = f"lower(coalesce(use_case, '')) LIKE '%{module.lower()}%'"
else:
    filter_clause = f"lower(coalesce(use_case, '')) LIKE '%{usecase.lower()}%'"


adb_latest_run_logs_df = spark.sql(f"""
WITH all_jobs AS (
    SELECT *,
           ROW_NUMBER() OVER (
                PARTITION BY job_name,table_name
                ORDER BY COALESCE(CAST(log_timestamp AS TIMESTAMP), TIMESTAMP '1900-01-01') DESC
    ) AS rn
    FROM transformation_{env}.etl_audit.etl_audit_logs_all
    WHERE source = 'DATABRICKS' 
      AND {filter_clause}
      AND job_run_date = current_date()
),

success_avg AS (
    SELECT 
        job_name,table_name,
        CAST(FLOOR(AVG(execution_duration) / 3600) AS STRING) || 'h ' ||
        CAST(FLOOR(MOD(AVG(execution_duration), 3600) / 60) AS STRING) || 'm ' ||
        CAST(FLOOR(MOD(AVG(execution_duration), 60)) AS STRING) || 's' AS avg_success_duration_hms
    FROM transformation_{env}.etl_audit.etl_audit_logs_all
    WHERE source = 'DATABRICKS'
      AND {filter_clause}
      AND result_state = 'SUCCESS'
      AND job_run_date >= current_date() - INTERVAL 30 DAYS
    GROUP BY job_name,table_name
)

SELECT 
    a.*,
    s.avg_success_duration_hms
FROM all_jobs a
LEFT JOIN success_avg s
  ON a.job_name = s.job_name and a.table_name = s.table_name
WHERE a.rn = 1
""").drop("rn")

# COMMAND ----------

# DBTITLE 1,get_all_workspace_url
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

# COMMAND ----------

from pyspark.sql.functions import when, col, lower, lit, regexp_extract, udf
from pyspark.sql.types import StringType

# Create a UDF to map workspace URL to source type
def get_source_type(run_page_url):
    if not run_page_url:
        return "UNKNOWN"
    
    # Extract workspace URL from run_page_url (format: https://<workspace_url>/?o=...)
    for ws_url, ws_name in workspace_url_map.items():
        if ws_url.lower() in run_page_url.lower():
            # Determine source based on workspace name pattern
            if "inges" in ws_name.lower():
                return "DBX INGES"
            elif "trans" in ws_name.lower():
                return "DBX TRANS"
            elif "ml" in ws_name.lower():
                return "DBX ML"
            else:
                return "DBX OTHER"
    
    return "UNKNOWN"

get_source_type_udf = udf(get_source_type, StringType())

adb_latest_run_logs_df = adb_latest_run_logs_df.withColumn(
    "source",
    get_source_type_udf(col("run_page_url"))
)

display(adb_latest_run_logs_df)

# COMMAND ----------

# DBTITLE 1,extract_failed_runs
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def extract_inprogress_runs(input_df: DataFrame) -> DataFrame:
    """
    Filters rows where result_state is an empty string,
    and returns a DataFrame with selected and aliased columns.

    Parameters:
    ----------
    input_df : pyspark.sql.DataFrame
        The input DataFrame to process.

    Returns:
    -------
    pyspark.sql.DataFrame
        Filtered and transformed DataFrame.
    """
    return (
        input_df.filter(col("result_state") == "")
                .select(
                    col("use_case").alias("tag"),
                    col("job_id").cast("long").alias("job_id"),
                    col("job_run_id").cast("long").alias("latest_run_id"),
                )
    )


# COMMAND ----------

# DBTITLE 1,enrich_with_task_aggregation
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import DataFrame, Row

def enrich_with_task_aggregation(master_df: DataFrame) -> DataFrame:
    from databricks.sdk import WorkspaceClient  
    workspace_client = WorkspaceClient()

    # Step 1: Rename column for processing
    parsed_df = master_df.withColumnRenamed("tasks_details", "parsed_tasks_details")

    # Step 2: Explode task array
    exploded = (
        parsed_df.withColumn("task", F.explode_outer("parsed_tasks_details"))
                 .withColumn("task_key", F.col("task.task_key"))
                 .withColumn("task_run_id", F.col("task.run_id"))
                 .withColumn("task_result_state", F.col("task.state.result_state"))
                 .withColumn("task_life_cycle_state", F.col("task.state.life_cycle_state"))
                 .withColumn("task_state_message", F.col("task.state.state_message"))
                 .withColumn("task_start_time", F.col("task.start_time"))
    )

    # Step 3: Keep only latest task run per job/task
    window_latest_task = Window.partitionBy("job_run_id", "task_key").orderBy(F.col("task_start_time").desc())
    latest_tasks = (
        exploded.withColumn("rn", F.row_number().over(window_latest_task))
                .filter(F.col("rn") == 1)
                .drop("rn")
    )

    # Step 4: Filter out intermediate states
    invalid_states = [
        "RunLifeCycleState.RUNNING", "RunLifeCycleState.PENDING",
        "RunLifeCycleState.BLOCKED", "RunLifeCycleState.WAITING_FOR_RETRY",
        "RunLifeCycleState.SKIPPED"
    ]
    valid_tasks = latest_tasks.filter(
        ~F.upper(F.trim(F.col("task_life_cycle_state"))).isin([s.upper() for s in invalid_states])
    )

    # Step 5: Filter for failed tasks
    failed_states = ["RunResultState.FAILED", "RunResultState.SUCCESS_WITH_FAILURES"]
    failure_tasks = valid_tasks.filter(
        F.upper(F.trim(F.col("task_result_state"))).isin([s.upper() for s in failed_states])
    )

    # Step 6: Get overall result state
    job_failure_flag = failure_tasks.groupBy("job_run_id").agg(F.count("*").alias("failure_count"))
    result_state_df = valid_tasks.select("job_run_id").distinct() \
        .join(job_failure_flag, on="job_run_id", how="left") \
        .withColumn(
            "result_state",
            F.when(F.col("failure_count") > 0, F.lit("FAILED")).otherwise(F.lit("SUCCESS"))
        ).drop("failure_count")

    # Step 7: Get first failed task (per job)
    fail_window = Window.partitionBy("job_run_id").orderBy(F.col("task_start_time").asc())
    first_failed_task = failure_tasks.withColumn("row", F.row_number().over(fail_window)) \
                                     .filter(F.col("row") == 1) \
                                     .select(
                                         "job_run_id",
                                         F.col("task_run_id").alias("failed_task_run_id"),
                                         F.col("task_state_message").alias("state_message")
                                     )

    # Step 8: Retrieve exact error message from get_run_output for each failed task
    failed_task_info = first_failed_task.collect()
    error_data = []
    for row in failed_task_info:
        run_id = row["failed_task_run_id"]
        job_run_id = row["job_run_id"]

        try:
            output = workspace_client.jobs.get_run_output(run_id=run_id)
            error_msg = output.error or row["state_message"] or "No error message"
        except Exception as e:
            error_msg = f"Error fetching error message: {str(e)}"

        error_data.append(Row(
            job_run_id=job_run_id,
            failed_task_run_id=run_id,
            state_message=error_msg
        ))

    # Convert enriched error info to DataFrame
    if error_data:
        enriched_failed_tasks_df = spark.createDataFrame(error_data)
    else:
        enriched_failed_tasks_df = spark.createDataFrame([], schema=StructType([
            StructField("job_run_id", LongType(), True),
            StructField("failed_task_run_id", LongType(), True),
            StructField("state_message", StringType(), True)
        ]))

    # Step 9: Combine final aggregation
    agg = result_state_df \
        .join(enriched_failed_tasks_df, on="job_run_id", how="left") \
        .withColumn("life_cycle_state", F.lit("RunLifeCycleState.TERMINATED")) \
        .withColumn("end_time", F.expr("current_timestamp() + INTERVAL 4 HOURS"))

    # Step 10: Join back to original master
    cleaned_df = parsed_df.drop("life_cycle_state", "result_state", "failed_task_run_id", "state_message", "end_time")
    updated_df = cleaned_df.join(agg, on="job_run_id", how="left")

    # Step 11: Rename and reorder
    final_df = updated_df.withColumnRenamed("parsed_tasks_details", "tasks_details") \
                         .select(*master_df.columns)

    return final_df


# COMMAND ----------

# DBTITLE 1,merge_to_delta_table
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql import DataFrame, SparkSession

def merge_to_delta_table(
    spark: SparkSession,
    source_df: DataFrame,
    table_name: str,
    merge_key: str = "job_run_id"
) -> None:
    """
    Merges the given source DataFrame into a Delta table using the specified merge key.

    Parameters:
    ----------
    spark : SparkSession
        Spark session object.
    
    source_df : DataFrame
        Source DataFrame to merge into the Delta table.
    
    table_name : str
        Fully qualified Delta table name (e.g., 'db.schema.table').
    
    merge_key : str, optional
        The column name to use as the merge condition key (default is 'job_run_id').

    Returns:
    -------
    None
    """
    delta_table = DeltaTable.forName(spark, table_name)

    # Get common columns
    target_cols = set(delta_table.toDF().columns)
    source_cols = set(source_df.columns)
    common_cols = list(target_cols.intersection(source_cols))

    # Prepare expressions
    update_expr = {c: col(f"source.{c}") for c in common_cols if c != merge_key}
    insert_expr = {c: col(f"source.{c}") for c in common_cols}

    # Perform merge
    (
        delta_table.alias("target")
        .merge(
            source=source_df.alias("source"),
            condition=col(f"target.{merge_key}") == col(f"source.{merge_key}")
        )
        .whenMatchedUpdate(set=update_expr)
        .whenNotMatchedInsert(values=insert_expr)
        .execute()
    )


# COMMAND ----------

# DBTITLE 1,join_adb_with_final_df
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def join_adb_with_final_df(adb_df: DataFrame, final_df: DataFrame) -> DataFrame:
    """
    Performs an inner join between adb_latest_run_logs_df and final_df on 'job_run_id',
    selecting specific columns from each and returning the merged DataFrame.

    Parameters:
    ----------
    adb_df : DataFrame
        The adb_latest_run_logs_df containing source/copy stats and tasks details.
    
    final_df : DataFrame
        The final_df containing job execution metadata.

    Returns:
    -------
    DataFrame
        Merged DataFrame with selected fields.
    """
    adb = adb_df.alias("adb")
    final = final_df.alias("final")

    merged = adb.join(final, on="job_run_id", how="inner").select(
        col("adb.source"),
        col("adb.use_case"),
        col("final.job_name"),
        col("final.job_id"),
        col("final.job_run_id"),
        col("final.job_run_date"),
        col("final.life_cycle_state"),
        col("final.result_state"),
        col("final.state_message"),
        col("final.start_time"),
        col("final.end_time"),
        col("final.setup_duration"),
        col("final.setup_duration_hms"),
        col("final.execution_duration"),
        col("final.execution_duration_hms"),
        col("adb.copy_activity_duration"),
        col("adb.copy_activity_duration_hms"),
        col("adb.data_source"),
        col("adb.table_name"),
        col("adb.source_data_bytes"),
        col("adb.data_written_bytes"),
        col("adb.source_records"),
        col("adb.records_read"),
        col("adb.records_copied"),
        col("adb.job_stats"),
        col("adb.activityType"),
        col("final.run_page_url"),
        col("adb.tasks_details"),
        col("final.failed_task_run_id"),
        col("final.log_timestamp"),
        col("adb.avg_success_duration_hms")
    )
    
    return merged


# COMMAND ----------

# DBTITLE 1,union_filtered_adb_with_merged
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def union_filtered_adb_with_merged(
    adb_df: DataFrame,
    merged_df: DataFrame,
    filter_col: str = "result_state"
) -> DataFrame:
    """
    Filters `adb_df` for non-empty `result_state`, aligns it with `merged_df` schema, 
    and performs a union.

    Parameters:
    ----------
    adb_df : DataFrame
        The adb_latest_run_logs_df to be filtered and aligned.

    merged_df : DataFrame
        The existing merged DataFrame with a defined schema.

    filter_col : str, optional
        The column to apply the non-null/non-empty filter on. Default is "result_state".

    Returns:
    -------
    DataFrame
        Combined DataFrame (adb_df filtered + merged_df).
    """
    # Step 1: Filter adb_df
    filtered_adb_df = adb_df.filter(
        (col(filter_col).isNotNull()) & (col(filter_col) != "")
    )

    # Step 2: Align schema
    filtered_aligned = filtered_adb_df.select(merged_df.columns)

    # Step 3: Union
    combined_df = merged_df.unionByName(filtered_aligned)

    return combined_df


# COMMAND ----------

from pyspark.sql.functions import col

# Check if there are rows with empty/null result_state
needs_processing = adb_latest_run_logs_df.filter(
    col("result_state").isNull() | (col("result_state") == "")
).limit(1).count() > 0

if needs_processing:
    # Step 1: Extract failed runs
    master_df_filtered = extract_inprogress_runs(adb_latest_run_logs_df)
    #display(master_df_filtered)

    # Step 2: Get job run details
    master_log_df_filtered = get_job_run_details(master_df_filtered, spark)
    #display(master_log_df_filtered)

    # Step 3: Enrich with task aggregation
    final_df = enrich_with_task_aggregation(master_log_df_filtered)
    display(final_df)

    # Step 4: Merge into Delta Table
    merge_to_delta_table(
        spark=spark,
        source_df=final_df,
        table_name=f"transformation_{env}.etl_audit.adb_job_runs",
        merge_key="job_run_id"
    )

    # Step 5: Join ADB with final_df
    merged_df = join_adb_with_final_df(adb_latest_run_logs_df, final_df)
    # display(merged_df)

    # Step 6: Union filtered ADB with merged_df
    adb_latest_run_logs_df = union_filtered_adb_with_merged(adb_latest_run_logs_df, merged_df)
    # display(adb_latest_run_logs_df)

else:
    print("✅ All result_state values are populated. No further processing required.")


# COMMAND ----------

from pyspark.sql.functions import col, min

# Replace {usecase} with the actual variable or string
# Get the minimum start_time for a given use_case where job_name is not null

min_start_time_row = adb_latest_run_logs_df \
    .filter((col("job_name").isNotNull()) & ((col("use_case") == usecase) | (col("use_case") == module))) \
    .agg(min("start_time").alias("min_start_time")) \
    .collect()

# Extract the timestamp value
master_job_start_time = min_start_time_row[0]["min_start_time"] if min_start_time_row else None

print(master_job_start_time)

# COMMAND ----------

adf_latest_run_logs_df = spark.sql(f"""
WITH today_jobs AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY log_timestamp DESC) AS rn
    FROM transformation_{env}.etl_audit.etl_audit_logs_all
    WHERE source = 'ADF'
      AND (lower(use_case) LIKE '{usecase}' OR lower(use_case) LIKE '{module}')
      AND end_time < '{master_job_start_time}'
      --AND job_run_date = current_date()
      AND job_run_date >= date_sub(current_date(), 1)

),

success_avg AS (
    SELECT
      table_name,
      CAST(FLOOR(AVG(execution_duration) / 3600) AS STRING) || 'h ' ||
      CAST(FLOOR(MOD(AVG(execution_duration), 3600) / 60) AS STRING) || 'm ' ||
      CAST(FLOOR(MOD(AVG(execution_duration), 60)) AS STRING) || 's' AS avg_duration_hms
    FROM transformation_{env}.etl_audit.etl_audit_logs_all
    WHERE source = 'ADF'
      AND (lower(use_case) LIKE '{usecase}' OR lower(use_case) LIKE '{module}')
      AND result_state = 'Succeeded'
      AND job_run_date >= current_date() - INTERVAL 30 DAYS
    GROUP BY table_name
)

SELECT tj.source,tj.use_case, concat(tj.job_name, '_', tj.table_name) as job_name, tj.job_id, tj.job_run_id, tj.job_run_date, tj.life_cycle_state, tj.result_state, tj.state_message, tj.start_time, tj.end_time, tj.setup_duration, tj.setup_duration_hms, tj.execution_duration, tj.execution_duration_hms, tj.copy_activity_duration, tj.copy_activity_duration_hms, tj.data_source, tj.table_name, tj.source_data_bytes, tj.data_written_bytes, tj.source_records, tj.records_read, tj.records_copied, tj.job_stats, tj.activityType, tj.run_page_url, tj.tasks_details, tj.failed_task_run_id, tj.log_timestamp, sa.avg_duration_hms
FROM today_jobs tj
LEFT JOIN success_avg sa ON tj.table_name = sa.table_name
WHERE rn = 1
""").drop("rn")

# COMMAND ----------

# display(adf_latest_run_logs_df)

# COMMAND ----------

from pyspark.sql.functions import get_json_object, concat, lit, col, concat_ws, when

combined_job_runs_df = adf_latest_run_logs_df.union(adb_latest_run_logs_df)

combined_job_runs_df = combined_job_runs_df.select(
    "source", 
    "use_case", 
    "job_name", 
    "job_id",
    "job_run_id",
    "job_run_date",
    "result_state",
    concat(
        lit("<div style='white-space: pre-wrap; word-wrap: break-word; text-align: justify;'>"),
        col("state_message"),
        lit("</div>")
    ).alias("state_message"),
    "start_time",
    "end_time",
    "execution_duration_hms",
    "table_name",
    concat_ws(
        "",
        lit("dim_new_records: "),
        get_json_object(col("job_stats"), "$.gold.dim_new_records"),
        lit("<br/>"),

        lit("dim_updated_records: "),
        get_json_object(col("job_stats"), "$.gold.dim_updated_records"),
        lit("<br/>"),

        lit("fact_inserted_records: "),
        get_json_object(col("job_stats"), "$.gold.fact_inserted_records"),
        lit("<br/>"),

        lit("fact_updated_records: "),
        get_json_object(col("job_stats"), "$.gold.fact_updated_records")
    ).alias("record_stats"),
    when(
    col("run_page_url").isNotNull(),
    concat_ws("", lit('<a href="'), col("run_page_url"), lit('" target="_blank">click to see</a>'))
    ).otherwise(lit("")).alias("run_page_url"),
    "failed_task_run_id",
    "avg_duration_hms",
    # Replace None with "N/A" for Databricks source
    when((col("source").contains("DBX")) & col("source_records").isNull(), lit("N/A")).otherwise(col("source_records")).alias("source_records"),
    when((col("source").contains("DBX")) & col("records_read").isNull(), lit("N/A")).otherwise(col("records_read")).alias("records_read"),
    when((col("source").contains("DBX")) & col("records_copied").isNull(), lit("N/A")).otherwise(col("records_copied")).alias("records_copied")
    )

# Drop duplicates based on key columns
combined_job_runs_df = combined_job_runs_df.dropDuplicates(
    ["job_name", "job_id", "job_run_id", "table_name"]
)
if usecase == "drr":
    combined_job_runs_df = combined_job_runs_df.filter(
        col("use_case") != "ea_drr"
    )

display(combined_job_runs_df)

# COMMAND ----------

from pyspark.sql.functions import lower

# Filter using the exact result_state value
success_df = (
    combined_job_runs_df.filter(
        (lower(col("result_state")) == "success") | 
        (lower(col("result_state")) == "succeeded")
    )
    .select(
        "source",
        "job_name",
        "job_id",
        "job_run_id",
        "job_run_date",
        "result_state",
        "start_time",
        "end_time",
        "execution_duration_hms",
        "table_name",
        "record_stats",
        "source_records",
        "records_read",
        "records_copied",
        "avg_duration_hms"
    )
)
success_count = success_df.count()

failure_df = combined_job_runs_df.filter(~lower(combined_job_runs_df.result_state).isin('success', 'succeeded'))
failure_count = failure_df.count()

# COMMAND ----------

# display(success_df)

# COMMAND ----------

# display(failure_df)

# COMMAND ----------

from datetime import datetime

# 1. Inputs
today_str = datetime.today().strftime("%Y-%m-%d")

# 2. Determine failure in either Databricks or Power BI
# If failure_count from Databricks > 0 OR if any Power BI report failed → status = Failed
powerbi_failure_count = latest_pbi_refresh_logs_df.filter(latest_pbi_refresh_logs_df.status != 'Completed').count()

if failure_count > 0 or powerbi_failure_count > 0:
    status = "Failed"
else:
    status = "Success"

# 3. Construct subject
# subject = (
#     f"[{status}] {usecase}"
#     + (f" - {module}" if module != "n/a" else "")
#     + f" - {env} - ETL Notification {today_str}"
# )
subject = (
    f"[{status}] {usecase}"
    + (f" - {module}" if module and module.lower() != "n/a" else "")
    + f" - {env} - ETL Notification {today_str}"
).upper()
print(subject)


# COMMAND ----------

import pyspark.sql.functions as F

# Convert success/failure DataFrames to HTML
success_html = success_df.toPandas().to_html(index=False, escape=False)
failure_html = failure_df.toPandas().to_html(index=False, escape=False)

# Use separate variable names for Power BI
pbi_success_count = latest_pbi_refresh_logs_df.filter(F.lower("status") == "completed").count()
pbi_failure_count = latest_pbi_refresh_logs_df.count() - pbi_success_count

#  Prepare the summary note for Power BI
note = f"""
<p><strong>Total successful refreshes:</strong> {pbi_success_count}<br>
<strong>Total failed refreshes:</strong> {pbi_failure_count}</p>
"""

# Convert table to HTML and append to note
pbi_html = note + latest_pbi_refresh_logs_df.toPandas().to_html(index=False, escape=False)


# COMMAND ----------

# 5. Styled HTML body with success/failure summary and all tables
message_body = f"""
<html>
<head>
<style>
    body {{
        font-family: Arial, sans-serif;
        font-size: 13px;
    }}
    .summary {{
        font-size: 15px;
        margin-bottom: 10px;
    }}
    table {{
        border: 1px solid black;
        border-collapse: collapse;
        width: 100%;
        table-layout: auto;
        margin-top: 10px;
    }}
    th, td {{
        border: 1px solid black;
        padding: 8px;
        text-align: center;
        white-space: nowrap;
    }}
    th {{
        background-color: #f2f2f2;
        font-weight: bold;
    }}
    h3 {{
        margin-top: 30px;
        font-size: 16px;
    }}
</style>
</head>
<body>

<p>Hi Team,</p>

<p>Please find the overall summary result for <b>{usecase}</b> execution:</p>
<p><b>Usecase:</b> {usecase}<br>
<b>Module:</b> {module}</p>

<div class="summary">
    ❌ <b>Total Jobs Failed:</b> {failure_count} <br>
    ✅ <b>Total Jobs Succeeded:</b> {success_count} <br>
</div>

<h3>❌ FAILURE JOBS</h3>
{failure_html}

<h3>✅ SUCCESS JOBS</h3>
{success_html}

<h3>📊 POWER BI REFRESH LOGS</h3>
{pbi_html}

<p>Regards,<br>Data Team</p>

</body>
</html>
"""

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(sender_email, sender_password, receiver_email, subject, html_content):
    try:
        msg = MIMEMultipart("alternative")
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject

        # Attach HTML content
        msg.attach(MIMEText(html_content, 'html'))

        # Send
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())

        print("✅ Email sent successfully!")

    except Exception as e:
        print(f"❌ Error sending email: {e}")

# Use secure secret for password
sender_email = "Data_team_notifications@miral.ae"
sender_password = dbutils.secrets.get(scope=secret_scope, key=f"{email_key}")
receiver_email = f"{receiver_email_id}"

# Send email
#send_email(sender_email, sender_password, receiver_email, subject, message_body)

if isinstance(receiver_email, str):
    recipient_list = [e.strip() for e in receiver_email.split(',') if e.strip()]
    #send_email(sender_email, sender_password, receiver_email, subject, message_body)
    #display(recipient_list)
else:
    recipient_list = [e.strip() for e in receiver_email if e.strip()]

for email in recipient_list:
    #print(email)
    send_email(sender_email, sender_password, email, subject, message_body)
