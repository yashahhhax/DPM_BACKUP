# Databricks notebook source
# DBTITLE 1,import library and function
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,import base function
# MAGIC %run ../common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,import utility_functions
# MAGIC %run ./logger_utility_functions

# COMMAND ----------

# DBTITLE 1,create widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("usecase","")
dbutils.widgets.text("env","")
dbutils.widgets.text("module","")
dbutils.widgets.text("workflow_name", "")
dbutils.widgets.text("current_job_run_id", "")

# COMMAND ----------

# DBTITLE 1,fetch env widgets value
try:
    notebook_vars = [
        'usecase',
        'env',
        'module',
        'workflow_name',
        'current_job_run_id'
    ]

    use_case = get_notebook_params(notebook_vars[0], verbose=True)
    env = get_notebook_params(notebook_vars[1], verbose=True)
    module = get_notebook_params(notebook_vars[2], verbose=True)
    workflow_name = get_notebook_params(notebook_vars[3], verbose=True)
    current_job_run_id = get_notebook_params(
        notebook_vars[4],
        verbose=True
    )

except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,defining catalog and schema
catalog = f"transformation_{env}"
etl_audit_schema="etl_audit"
gld_schema='gld_procurement'
audit_table = f"{catalog}.{etl_audit_schema}.etl_audit_logs_all"
audit_df= spark.table(audit_table)

# COMMAND ----------

# DBTITLE 1,Finding Latest ADB Run Logs
# Construct dynamic WHERE filter

if module:
    filter_clause = f"lower(coalesce(use_case, '')) LIKE '%{module.lower()}%'"
else:
    filter_clause = f"lower(coalesce(use_case, '')) LIKE '%{use_case.lower()}%'"


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

# DBTITLE 1,Exact Failed Runs
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

# DBTITLE 1,Enrich_with_task_aggregation
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

    display(final_df)


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

# DBTITLE 1,Fetching the Final Status of Workflow
from pyspark.sql.functions import col

display(adb_latest_run_logs_df)

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

else:
    print("✅ All result_state values are populated. No further processing required.")

