# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC **Generic KPI snapshot orchestrator notebook.**
# MAGIC Checks last available KPI date and backloads missing dates day by day.
# MAGIC
# MAGIC ### Parameters
# MAGIC | Parameter | Options | Description |
# MAGIC | --- | --- | --- |
# MAGIC | kpi_level | customer / asset / group | Which KPI notebook to trigger |
# MAGIC | identifier_type | email_id / omni_sk | For customer KPI only |
# MAGIC
# MAGIC ### Workflow Design (4 parallel tasks)
# MAGIC | Task | kpi_level | identifier_type |
# MAGIC | --- | --- | --- |
# MAGIC | Task 1 | customer | email_id |
# MAGIC | Task 2 | customer | omni_sk |
# MAGIC | Task 3 | asset | — |
# MAGIC | Task 4 | group | — |
# MAGIC
# MAGIC ### Logic
# MAGIC 1. Get MAX(report_date) from target table
# MAGIC 2. Generate date list from last_date + 1 to yesterday
# MAGIC 3. Loop and trigger KPI notebook for each missing date

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
# dbutils.widgets.text("env", "", "")
# dbutils.widgets.text("kpi_level", "", "")
# dbutils.widgets.text("identifier_type", "", "")
# dbutils.widgets.text("current_job_run_id", "", "")
# dbutils.widgets.text("report_date", "", "")

# COMMAND ----------

# DBTITLE 1,include base functions
# MAGIC %run ../../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,get notebook parameters
try:
    notebook_vars = [
        'env', 'kpi_level', 'identifier_type', 'current_job_run_id', 'report_date'
    ]
    env, kpi_level, identifier_type, current_job_run_id, report_date = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,import dependencies
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# DBTITLE 1,date utility function
def get_list_of_days_between_dates(start_date, end_date,existing_dates) -> list:
    """
    Returns list of dates from start_date + 1 day to end_date (inclusive).
    start_date is exclusive — end_date is inclusive.
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    date_list = []
    if not existing_dates:
        current   = start_date
    else:
        current   = start_date + timedelta(days=1)

    while current <= end_date:
        date_list.append(str(current))
        current += timedelta(days=1)
    return date_list

# COMMAND ----------

# DBTITLE 1,initialize parameters
#--------------------notebook paths — relative to this notebook location--------------------
# NOTEBOOK_BASE_PATH         = f"{notebook_base_path}"
NOTEBOOK_BASE_PATH         = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0]
NOTEBOOK_PATH_CUSTOMER_KPI = f"{NOTEBOOK_BASE_PATH}/plt_customer_kpi"
NOTEBOOK_PATH_ASSET_KPI    = f"{NOTEBOOK_BASE_PATH}/plt_asset_kpi"
NOTEBOOK_PATH_GROUP_KPI    = f"{NOTEBOOK_BASE_PATH}/plt_group_kpi"
NOTEBOOK_DEFAULT_TIMEOUT_SEC = 14400  # 4 hours per notebook run

#--------------------target tables--------------------
tgt_db             = f"transformation_{env}.plt_customer360"
customer_kpi_table = f"{tgt_db}.customer_kpi"
asset_kpi_table    = f"{tgt_db}.asset_kpi"
group_kpi_table    = f"{tgt_db}.group_kpi"

#--------------------yesterday — KPIs always populated for yesterday--------------------
snapshot_date = str((datetime.now(ZoneInfo("Asia/Dubai")) - timedelta(days=1)).date())

print(f"env                  : {env}")
print(f"kpi_level            : {kpi_level}")
print(f"identifier_type      : {identifier_type}")
print(f"snapshot_date        : {snapshot_date}")
print(f"NOTEBOOK_BASE_PATH : {NOTEBOOK_BASE_PATH}")

# COMMAND ----------

print(f"NOTEBOOK_BASE_PATH   : {NOTEBOOK_BASE_PATH}")
print(f"NOTEBOOK_PATH_CUSTOMER_KPI : {NOTEBOOK_PATH_CUSTOMER_KPI}")
print(f"NOTEBOOK_PATH_ASSET_KPI    : {NOTEBOOK_PATH_ASSET_KPI}")
print(f"NOTEBOOK_PATH_GROUP_KPI    : {NOTEBOOK_PATH_GROUP_KPI}")

# COMMAND ----------

# DBTITLE 1,determine target table and notebook path
if kpi_level == 'customer':
    target_table  = customer_kpi_table
    notebook_path = NOTEBOOK_PATH_CUSTOMER_KPI
    #--------------------validate identifier_type for customer KPI--------------------
    valid_identifier_types = ['email_id', 'omni_sk','scv_sk']
    if identifier_type not in valid_identifier_types:
        raise Exception(f"Invalid identifier_type: '{identifier_type}'. Must be one of {valid_identifier_types}")
elif kpi_level == 'asset':
    target_table  = asset_kpi_table
    notebook_path = NOTEBOOK_PATH_ASSET_KPI
elif kpi_level == 'group':
    target_table  = group_kpi_table
    notebook_path = NOTEBOOK_PATH_GROUP_KPI
else:
    raise Exception(f"Invalid kpi_level: '{kpi_level}'. Must be customer / asset / group")

print(f"target_table  : {target_table}")
print(f"notebook_path : {notebook_path}")

# COMMAND ----------

# DBTITLE 1,get max kpi date from target table
try:
    if kpi_level == 'customer':
        #--------------------get all existing dates for this identifier_type--------------------
        existing_dates_df = spark.sql(f"""
            SELECT DISTINCT cast(report_date as string) AS report_date
            FROM {target_table}
            WHERE identifier_type = '{identifier_type}'
        """)
    else:
        #--------------------asset/group KPI — no identifier_type filter--------------------
        existing_dates_df = spark.sql(f"""
            SELECT DISTINCT cast(report_date as string) AS report_date
            FROM {target_table}
        """)

    existing_dates = {row[0] for row in existing_dates_df.collect()}

    if not existing_dates:
        #--------------------table empty — default to yesterday--------------------
        min_date = snapshot_date
        max_date = snapshot_date
        print(f"Table empty — defaulting to yesterday: {snapshot_date}")
    else:
        min_date = str(min(existing_dates))
        max_date = str(max(existing_dates))
        # print(f"Existing dates    : {sorted(existing_dates)}")
        print(f"Min date          : {min_date}")
        print(f"Max date          : {max_date}")

except AnalysisException as e:
    #--------------------table not found — expected on first run--------------------
    existing_dates = set()
    min_date       = snapshot_date
    max_date       = snapshot_date
    print(f"Table not found — defaulting to yesterday: {snapshot_date}")

except Exception as e:
    #--------------------unexpected error -------------------
    print(f"Unexpected error accessing {target_table}: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,generate missing date list
#--------------------if report_date passed from workflow — check if exists--------------------
if report_date and str(report_date).strip() and str(report_date).strip() != 'None':
    report_date = report_date.strip()
    print(f"Loading for provided report date: {report_date}")
    if report_date in existing_dates:
        print(f"Data already exists for {report_date} — reloading ")
        date_list = [report_date]
    else:
        print(f"Data not found for {report_date} — loading")
        date_list = [report_date]
else:
    #--------------------auto detect ALL missing dates between min and snapshot_date--------------------
    all_dates  = get_list_of_days_between_dates(min_date, snapshot_date,existing_dates)
    date_list  = [d for d in all_dates if d not in existing_dates]

if not date_list:
    print(f"No missing dates — KPIs already up to date for {snapshot_date} ")
else:
    print(f"Dates to backload ({len(date_list)}) : {date_list}")

# COMMAND ----------

# DBTITLE 1,trigger KPI notebook for each missing date
if date_list:
    failed_dates  = []
    success_dates = []

    for kpi_date in date_list:
        max_retries = 2
        attempt     = 0
        success     = False

        while attempt < max_retries and not success:
            try:
                attempt += 1
                print(f"Triggering {kpi_level} KPI | date: {kpi_date} | attempt: {attempt} | identifier_type: {identifier_type}")

                #--------------------build notebook arguments--------------------
                notebook_args = {
                    "env"                : env,
                    "report_date"        : str(kpi_date),
                    "current_job_run_id" : current_job_run_id
                }

                #--------------------add identifier_type for customer KPI only--------------------
                if kpi_level == 'customer':
                    notebook_args["identifier_type"] = identifier_type

                print(f"notebook_args : {notebook_args}")
                print(f"kpi_date      : {kpi_date}")
                print(f"notebook_path : {notebook_path}")

                #--------------------trigger KPI notebook--------------------
                result = dbutils.notebook.run(
                    path            = notebook_path,
                    timeout_seconds = NOTEBOOK_DEFAULT_TIMEOUT_SEC,
                    arguments       = notebook_args
                )

                #--------------------check if child exited with error--------------------
                if result and result.startswith("FAILED:"):
                    raise Exception(result)

                success_dates.append(kpi_date)
                print(f"{kpi_level} KPI loaded for {kpi_date}")
                success = True

            except Exception as e:
                #--------------------print actual error from child notebook--------------------
                error_msg = str(e)
                print(f"Attempt {attempt} failed for {kpi_date}: {error_msg}")
                if "No data found" in error_msg:
                    failed_dates.append(kpi_date)
                    break
                if attempt >= max_retries:
                    failed_dates.append(kpi_date)
                    print(f"Max retries reached for {kpi_date}")

        #--------------------stop on first failure — don't continue to next date--------------------
        if failed_dates:
            break

    print(f"\nBackload complete")
    print(f"Success dates ({len(success_dates)}) : {success_dates}")
    print(f"Failed dates  ({len(failed_dates)})  : {failed_dates}")

    if failed_dates:
        raise Exception(f"Following dates failed after {attempt} attempts: {failed_dates}. Last error: {error_msg}")
else:
    print(f"Nothing to backload")

# COMMAND ----------

# DBTITLE 1,tbl readiness - update tag
table_name = target_table.split('.')[-1]

if kpi_level == 'customer': readiness_table = f"transformation_{env}.plt_customer360.readiness_{table_name}_{identifier_type}"
else: readiness_table = f"transformation_{env}.plt_customer360.readiness_{table_name}"

df = spark.createDataFrame([(spark.sql("SELECT from_utc_timestamp(current_timestamp(), 'Asia/Dubai')").collect()[0][0],)],["last_updated_timestamp"])
display(df)

df.write.mode("append").saveAsTable(readiness_table)
add_last_updated_tag(readiness_table)

# COMMAND ----------

# DBTITLE 1,trigger orchestration
trigger_databricks_job_by_name('workflow_orchestration')
