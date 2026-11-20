# Databricks notebook source

workspace_url_map = {
    # url: (workspace_name, catalog_name)

    # --- Ingestion Workspaces ---
    "adb-2766497291955675.15.azuredatabricks.net" : ("dbw-mx-data-adb-inges-dev-01",    "ingestion_dev"),
    "adb-1199415078036138.18.azuredatabricks.net" : ("dbw-mx-data-adb-inges-idev-01",   "ingestion_idev"),
    "adb-2731791389375139.19.azuredatabricks.net" : ("dbw-mx-data-adb-inges-prd-01",    "ingestion_prd"),
    "adb-1321531737024345.5.azuredatabricks.net"  : ("dbw-mx-data-adb-inges-preprd-01", "ingestion_preprd"),
    "adb-1628851157633194.14.azuredatabricks.net" : ("dbw-mx-data-adb-inges-uat-01",    "ingestion_uat"),

    # --- Transformation Workspaces ---
    "adb-485279261696768.8.azuredatabricks.net"   : ("dbw-mx-data-adb-trans-dev-01",    "transformation_dev"),
    "adb-2424669970410431.11.azuredatabricks.net" : ("dbw-mx-data-adb-trans-idev-01",   "transformation_idev"),
    "adb-1645531006591813.13.azuredatabricks.net" : ("dbw-mx-data-adb-trans-prd-01",    "transformation_prd"),
    "adb-635866415826582.2.azuredatabricks.net"   : ("dbw-mx-data-adb-trans-preprd-01", "transformation_preprd"),
    "adb-125701397626662.2.azuredatabricks.net"   : ("dbw-mx-data-adb-trans-uat-01",    "transformation_uat"),

    # --- ML Workspaces ---
    "adb-3821540461636682.2.azuredatabricks.net"  : ("dbw-mx-data-adb-ml-dev-01",       "ml_dev"),
    "adb-998748609691736.16.azuredatabricks.net"  : ("dbw-mx-data-adb-ml-idev-01",      "ml_idev"),
    "adb-1630099358760621.1.azuredatabricks.net"  : ("dbw-mx-data-adb-ml-prd-01",       "ml_prd"),
    "adb-3859238561111997.17.azuredatabricks.net" : ("dbw-mx-data-adb-ml-preprd-01",    "ml_preprd"),
    "adb-4291857358528928.8.azuredatabricks.net"  : ("dbw-mx-data-adb-ml-uat-01",       "ml_uat"),

    # --- UC Admin ---
    "adb-723762768397429.9.azuredatabricks.net"   : ("dbw-mx-data-adb-uc-admin-prd-01", "uc_admin_prd"),
}

def get_workspace_info(spark) -> dict:
    """
    Resolves workspace metadata from the current workspace URL.

    Returns:
        dict with keys:
            - workspace_url  : full azuredatabricks.net URL
            - workspace_name : human-readable Azure resource name
            - catalog_name   : Unity Catalog name for this workspace

    Raises:
        ValueError: If workspace URL is not found in the map.
    """
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

    if workspace_url not in workspace_url_map:
        raise ValueError(
            f"[ERROR] Workspace URL '{workspace_url}' not found in workspace_url_map. "
            f"Please add it to the map in logger_utility_functions."
        )

    workspace_name, catalog_name = workspace_url_map[workspace_url]

    print(f"[INFO] Workspace URL  : {workspace_url}")
    print(f"[INFO] Workspace name : {workspace_name}")
    print(f"[INFO] Catalog name   : {catalog_name}")

    return {
        "workspace_url" : workspace_url,
        "workspace_name": workspace_name,
        "catalog_name"  : catalog_name,
    }


# COMMAND ----------

# DBTITLE 1,get_notebook_params
def get_notebook_params(*param_names, verbose=False):

    values = []

    for name in param_names:
        try:
            value = dbutils.widgets.get(name)
        except:
            value = None
            if verbose:
                print(f"[INFO] Widget '{name}' not found. Defaulting to default value.")
        if verbose:
            print(f"[INFO] {name} = {value}")
        values.append(value)

    return values[0] if len(values) == 1 else values



# COMMAND ----------

# DBTITLE 1,get_latest_run_ids_for_job_names
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import current_timestamp

def get_latest_run_ids_for_job_names(job_name_tag_pairs: list[tuple[str, str]], spark: SparkSession) -> "pyspark.sql.DataFrame":
    """
    Fetch job_name, job_id, and latest run_id for a given list of job names using WorkspaceClient.

    Args:
        job_name_tag_pairs: list[tuple[str, str]]: List of job names to fetch.
        spark (SparkSession): Active Spark session.

    Returns:
        pyspark.sql.DataFrame: DataFrame with job_name, tag, job_id, latest_run_id.
    """
    # Initialize WorkspaceClient using default environment variables or config
    w = WorkspaceClient()

    results = []
    job_name_to_id = {}

    # Extract unique job names
    job_names = list(set([name for name, _ in job_name_tag_pairs]))

    # Step 1: Map job names to job_ids
    for job in w.jobs.list():  # Lazy iterator
        if job.settings.name in job_names:
            job_name_to_id[job.settings.name] = job.job_id

    # Step 2: Get latest run_id for each matched job with its tag
    for job_name, tag in job_name_tag_pairs:
        job_id = job_name_to_id.get(job_name)
        if job_id is None:
            print(f"Job name not found: {job_name}")
            results.append((job_name, tag, None, None))
            continue

        # Fetch latest run (limit=1)
        runs = list(w.jobs.list_runs(job_id=job_id, limit=1))
        latest_run_id = runs[0].run_id if runs else None
        results.append((job_name, tag, job_id, latest_run_id))

    # Step 3: Build Spark DataFrame
    schema = StructType([
        StructField("job_name", StringType(), True),
        StructField("tag", StringType(), True),
        StructField("job_id", LongType(), True),
        StructField("latest_run_id", LongType(), True),
    ])

    result_df = spark.createDataFrame(results, schema)
    result_df = result_df.withColumn("last_updated_timestamp", current_timestamp())

    return result_df


# COMMAND ----------

# DBTITLE 1,get_job_run_details
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, explode, first, when, lit, coalesce, to_date
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient


def get_job_run_details(run_ids_df, spark: SparkSession):
    workspace_client = WorkspaceClient()

    results = []

    # Define schema once
    job_run_log_schema = StructType([
        StructField("usecase", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("job_id", LongType(), True),
        StructField("job_run_id", LongType(), True),
        StructField("job_run_date", StringType(), True),
        StructField("life_cycle_state", StringType(), True),
        StructField("result_state", StringType(), True),
        StructField("state_message", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("setup_duration", LongType(), True),
        StructField("setup_duration_hms", StringType(), True),
        StructField("execution_duration", LongType(), True),
        StructField("execution_duration_hms", StringType(), True),
        StructField("run_page_url", StringType(), True),
        StructField("tasks_details", ArrayType(
            StructType([
                StructField("run_id", LongType(), True),
                StructField("task_key", StringType(), True),
                StructField("run_page_url", StringType(), True),
                StructField("start_time", StringType(), True),
                StructField("end_time", StringType(), True),
                StructField("execution_duration", StringType(), True),
                StructField("state", StructType([
                    StructField("life_cycle_state", StringType(), True),
                    StructField("result_state", StringType(), True),
                    StructField("state_message", StringType(), True),
                    StructField("user_cancelled_or_timedout", BooleanType(), True)
                ]), True)
            ])
        ), True),
        StructField("failed_task_run_id", LongType(), True),
        StructField("log_timestamp", TimestampType(), True)
    ])

    # Collect all job runs
    run_id_list = run_ids_df.select("tag", "job_id", "latest_run_id").distinct().collect()

    for row in run_id_list:
        tag = row["tag"]
        job_id = row["job_id"]
        job_run_id = row["latest_run_id"]

        try:
            run = workspace_client.jobs.get_run(run_id=job_run_id)
        except Exception as e:
            print(f"Failed to fetch run {job_run_id}: {str(e)}")
            continue

        results.append(Row(
            usecase = tag,
            job_name=run.run_name,
            job_id=job_id,
            job_run_id=job_run_id,
            job_run_date=run.start_time,
            life_cycle_state=run.state.life_cycle_state if run.state else None,
            result_state=run.state.result_state if run.state else None,
            state_message=run.state.state_message if run.state else None,
            start_time=run.start_time,
            end_time=run.end_time,
            setup_duration=(run.setup_duration // 1000) if run.setup_duration else None,
            setup_duration_hms=(str(run.setup_duration // 3600000) + "h " + str((run.setup_duration % 3600000) // 60000) + "m " + str((run.setup_duration % 60000) // 1000) + "s") if run.setup_duration else None,
            execution_duration=(run.run_duration // 1000) if run.run_duration else None,
            execution_duration_hms=(str(run.run_duration // 3600000) + "h " + str((run.run_duration % 3600000) // 60000) + "m " + str((run.run_duration % 60000) // 1000) + "s") if run.run_duration else None,
            run_page_url=run.run_page_url,
            tasks_details=run.tasks if hasattr(run, "tasks") else [],
            failed_task_run_id=None,
            log_timestamp=datetime.now(timezone.utc)
        ))

    if not results:
        print("No valid runs fetched.")
        return None

    df = spark.createDataFrame(results, job_run_log_schema)

    failed_jobs_df = df.filter(col("result_state").isin("RunResultState.SUCCESS_WITH_FAILURES", "RunResultState.FAILED", "RunResultState.TIMEDOUT", "RunResultState.CANCELED"))
    # display(failed_jobs_df)

    if failed_jobs_df.count() > 0:
        # Explode tasks and get failed task run_id
        failed_tasks = failed_jobs_df \
            .withColumn("task", explode("tasks_details")) \
            .filter(col("task.state.result_state").isin("RunResultState.FAILED", "RunResultState.CANCELED", "RunResultState.TIMEDOUT")) \
            .select("job_run_id", col("task.run_id").alias("first_failed_task_run_id"))
        # display(failed_tasks)

        first_failed_per_job = failed_tasks.groupBy("job_run_id") \
            .agg(first("first_failed_task_run_id").alias("joined_failed_task_run_id"))

        # Join back to full df
        df_with_failed = df.join(first_failed_per_job, on="job_run_id", how="left") \
            .withColumn(
                "failed_task_run_id",
                when(
                    col("result_state").isin("RunResultState.SUCCESS_WITH_FAILURES", "RunResultState.FAILED", "RunResultState.CANCELED", "RunResultState.TIMEDOUT"),
                    col("joined_failed_task_run_id")
                ).otherwise(None)
            ).drop("joined_failed_task_run_id")
        
        # display(df_with_failed)

        failed_only = df_with_failed.filter(col("result_state").isin("RunResultState.SUCCESS_WITH_FAILURES", "RunResultState.FAILED", "RunResultState.TIMEDOUT", "RunResultState.CANCELED"))
        # display(failed_only)
        enriched_failed = process_failed_jobs(failed_only, workspace_client)
        # display(enriched_failed)

        df_clean = df_with_failed.drop("state_message")
        # display(df_clean)

        full_final_df = df_clean.alias("base").join(
            enriched_failed.select("job_run_id", "state_message").alias("enriched"),
            on="job_run_id",
            how="left"
        ).withColumn(
            "state_message", coalesce(col("enriched.state_message"), lit(None))
        )
        # display(full_final_df)

    else:
        full_final_df = df
        print("No failed jobs found.")

    ordered_cols = [
        'usecase','job_name', 'job_id', 'job_run_id', 'job_run_date', 'life_cycle_state',
        'result_state', 'state_message', 'start_time', 'end_time',
        'setup_duration', 'setup_duration_hms', 'execution_duration', 'execution_duration_hms', 'run_page_url', 'tasks_details',
        'failed_task_run_id', 'log_timestamp'
    ]

    full_final_df = full_final_df.select(*ordered_cols)

    from pyspark.sql.functions import from_utc_timestamp

    # Convert from UTC to UAE (GST / Asia/Dubai)
    full_final_df = full_final_df \
        .withColumn("job_run_date", from_utc_timestamp((col("job_run_date") / 1000).cast("timestamp"), "Asia/Dubai").cast("date")) \
        .withColumn("start_time", from_utc_timestamp((col("start_time") / 1000).cast("timestamp"), "Asia/Dubai")) \
        .withColumn("end_time", from_utc_timestamp((col("end_time") / 1000).cast("timestamp"), "Asia/Dubai"))


    return full_final_df


# COMMAND ----------

# DBTITLE 1,process_failed_jobs
from pyspark.sql.types import StructType
from pyspark.sql import Row
from pyspark.sql.functions import col

def process_failed_jobs(df, workspace_client):
    """
    Enrich the failed job runs by fetching the state_message from their failed task run output
    using the Databricks WorkspaceClient.

    Args:
        df (DataFrame): Filtered DataFrame of failed jobs.
        workspace_client (WorkspaceClient): Authenticated Databricks workspace client.

    Returns:
        DataFrame: DataFrame with updated state_message column for failed jobs.
    """
    # Filter only rows with failed or canceled result_state
    failed_jobs_df = df.filter(
        col("result_state").isin("RunResultState.SUCCESS_WITH_FAILURES", "RunResultState.FAILED", "RunResultState.TIMEDOUT", "RunResultState.CANCELED")
    )

    failed_rows = failed_jobs_df.collect()
    # print(failed_rows)
    schema = df.schema

    augmented_rows = []
    for row in failed_rows:
        run_id = row["failed_task_run_id"]
        print(run_id)
        if run_id is None:
            continue

        # Use WorkspaceClient to get output
        try:
            run_output = workspace_client.jobs.get_run_output(run_id=run_id)
            # error_details = run_output.metadata.state.state_message
            error_details = run_output.error
            # print(error_details)
        except Exception as e:
            error_details = f"Failed to retrieve run output: {str(e)}"

        # Update row with new state_message
        new_row = row.asDict()
        new_row["state_message"] = error_details
        augmented_rows.append(Row(**new_row))

    if not augmented_rows:
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(augmented_rows, schema)


# COMMAND ----------

# DBTITLE 1,get_run_output
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
