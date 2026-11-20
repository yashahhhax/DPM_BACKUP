# Databricks notebook source
# DBTITLE 1,widgets
dbutils.widgets.text("pbi_client_id", "")
dbutils.widgets.text("pbi_azure_key_vault", "")
dbutils.widgets.text("pbi_secret_name", "")
dbutils.widgets.text("pbi_tenant_name", "")
dbutils.widgets.text("env", "")
dbutils.widgets.text("usecase", "")
dbutils.widgets.text("modules", "")
dbutils.widgets.text("power_bi_refresh_required", "yes", "Power BI Refresh Required")

# COMMAND ----------

# Databricks widget setup
refresh_flag = dbutils.widgets.get("power_bi_refresh_required").strip().lower()

# Treat empty input as "yes"
if refresh_flag == "":
    refresh_flag = "yes"

# Define normalized acceptable values
yes_values = [ "y","yes"]
no_values  = [ "n", 'no']

# Conditional logic
if refresh_flag in no_values:
    print(f"Skipping notebook execution since Power BI refresh is not required. (Input: '{refresh_flag}')")
    dbutils.notebook.exit("Skipped as per widget setting")

elif refresh_flag in yes_values:
    print(f"Proceeding with Power BI refresh as requested. (Input: '{refresh_flag}')")

else:
    print(f"⚠️ Invalid widget value '{refresh_flag}'. Expected Y/Yes or N/No.")
    dbutils.notebook.exit("Skipped due to invalid input.")


# COMMAND ----------

# MAGIC %run ../logging/etl_alert_config

# COMMAND ----------

# DBTITLE 1,import base functions
# MAGIC %run ./base_functions

# COMMAND ----------

# DBTITLE 1,import etl util functions for powerbi dataset refresh
# MAGIC %run ./etl_utils_powerbi

# COMMAND ----------

# DBTITLE 1,import etl util functions to send teams notifications
# MAGIC %run ./etl_utils_teams

# COMMAND ----------

# DBTITLE 1,Read notebook params
try:
    notebook_vars = [
        'pbi_client_id',
        'pbi_azure_key_vault',
        'pbi_secret_name',
        'pbi_tenant_name',
        'env',
        'usecase',
        'modules'
    ]
    pbi_client_id, pbi_azure_key_vault, pbi_secret_name, pbi_tenant_name, env, usecase, modules = get_notebook_params(*notebook_vars, verbose=True)
    usecase=usecase.lower()
except Exception as e:
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------
# Check if any datasets for this usecase have refresh_required = 'Y'
refresh_check_df = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM transformation_{env}.config.pbi_datasets
    WHERE LOWER(usecase) = LOWER('{usecase}')
      AND UPPER(refresh_required) = 'Y'
""")

refresh_count = refresh_check_df.collect()[0]["cnt"]

if refresh_count == 0:
    print(f"No datasets with refresh_required = 'Y' for usecase '{usecase}'. Skipping notebook.")
    dbutils.notebook.exit("Skipped: refresh_required is N for all datasets in this usecase.")
else:
    print(f"Found {refresh_count} dataset(s) with refresh_required = 'Y' for usecase '{usecase}'. Proceeding with refresh.")

# COMMAND ----------
# DBTITLE 1,install msal (Microsoft Authentication Library) library
# MAGIC %pip install msal

# COMMAND ----------

# DBTITLE 1,import required libraries, functions
import msal
import requests
import json
import pandas as pd

# COMMAND ----------

# DBTITLE 1,define dpm_pbi_refresh_logs table
catalog = f"transformation_{env}"
print(catalog)
schema = "etl_audit"
pbi_refresh_table = "pbi_refresh_history"
pbi_refresh_log_table = f"{catalog}.{schema}.{pbi_refresh_table}"

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Extract usecase name and dataset names
pbi_datasets_refresh_df = spark.sql(f"""
   WITH required_dataset_refresh AS
(
  SELECT *
  FROM transformation_{env}.config.pbi_datasets
  WHERE LOWER(usecase) = LOWER('{usecase}')
      AND UPPER(refresh_required) = 'Y'
)
    SELECT prh.*
    FROM required_dataset_refresh rdr 
    left join transformation_{env}.etl_audit.pbi_refresh_history prh
      ON lower(rdr.usecase) = lower(prh.usecase) 
      and  rdr.dataset_id = prh.dataset_id
    WHERE lower(rdr.usecase) = lower('{usecase}')
      AND status = 'Unknown' and DATE(start_time) >= CURRENT_DATE()-1
""")

# COMMAND ----------

# DBTITLE 1,define scope, authority url
#Set parameters
pbi_client_secret =  dbutils.secrets.get(pbi_azure_key_vault, pbi_secret_name)
pbi_authority_url = f"https://login.microsoftonline.com/{pbi_tenant_name}"
pbi_scope = ["https://analysis.windows.net/powerbi/api/.default"]

# COMMAND ----------

# DBTITLE 1,generate auth token
#Use MSAL to grab token
app = msal.ConfidentialClientApplication(pbi_client_id, authority=pbi_authority_url, client_credential=pbi_client_secret)
result = app.acquire_token_for_client(scopes=pbi_scope)
# print(result)
access_token = result['access_token']

# COMMAND ----------

# Build webhook_url dynamically
webhook_url = f"{env}_{usecase}_webhook_url"

# Get the actual value from globals()
webhook_url = globals()[webhook_url]

# COMMAND ----------

# DBTITLE 1,poll refresh status untill all completes
import requests
import pandas as pd

# Step 1: Extract list of (usecase, workspace_id, dataset_name, dataset_id) tuples
dataset_tuples = pbi_datasets_refresh_df.select("usecase", "workspace_id", "report_name", "dataset_id") \
                                        .distinct().toPandas()
dataset_tuples = list(dataset_tuples.itertuples(index=False, name=None))
print(dataset_tuples)

# Step 2: Handle empty list
if not dataset_tuples:
    print("No dataset IDs provided to check refresh status.")
else:
    # Step 3: Fetch latest refresh info into refresh_df
    refresh_df = check_powerbi_refresh_status(dataset_tuples, access_token)
    from pyspark.sql.functions import lit

    refresh_df_filtered = refresh_df.filter("status != 'Unknown'") \
                                    .select("refresh_date", "usecase", "report_name", "status", "error_message", "refresh_duration_hms")
    # display(refresh_df_filtered)

    if refresh_df_filtered.count() > 0:
        print("Sending teams notification...")
        send_teams_notification(refresh_df_filtered, webhook_url, usecase, env)

    # Step 4: Merge into audit table (even if some refreshes failed or unknown)
    merge_refresh_status_to_delta(refresh_df, pbi_refresh_log_table)

    # Step 5: Check status and raise error if any "Unknown"
    if refresh_df and refresh_df.count() > 0:
        pandas_df = refresh_df.select("workspace_id", "dataset_id", "status").toPandas()

        unknown_datasets = pandas_df[pandas_df["status"] == "Unknown"]

        if not unknown_datasets.empty:
            print("These datasets are still in 'Unknown' status:")
            for _, row in unknown_datasets.iterrows():
                print(f"Workspace: {row['workspace_id']} | Dataset: {row['dataset_id']}")
            raise RuntimeError("One or more datasets are in 'Unknown' status. Failing the task.")

    print("All dataset refreshes are completed (or failed). Nothing more to do.")


# COMMAND ----------

# from pyspark.sql import functions as F
# from pyspark.sql.window import Window

# # Read the table
# pbi_reresh_logs_df = spark.table(f"transformation_{env}.etl_audit.dpm_pbi_refresh_logs") \
#           .filter((F.col("usecase") == usecase) & (F.col("refresh_date") == F.current_date()))

# # Define window partitioned by dataset_id ordered by latest log_timestamp
# window_spec = Window.partitionBy("dataset_id").orderBy(F.col("log_timestamp").desc())

# # Add row number and filter for latest
# latest_pbi_refresh_logs_df = pbi_reresh_logs_df.withColumn("row_num", F.row_number().over(window_spec)) \
#               .filter(F.col("row_num") == 1) \
#               .drop("row_num")

# # display(latest_pbi_refresh_logs_df)

# send_powerbi_refresh_summary(latest_pbi_refresh_logs_df, webhook_url, usecase, env)
