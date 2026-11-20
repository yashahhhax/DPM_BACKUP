# Databricks notebook source
# DBTITLE 1,widgets
# dbutils.widgets.text("client_id", "")
# dbutils.widgets.text("azure_key_vault", "")
# dbutils.widgets.text("secret_name", "")
# dbutils.widgets.text("tenant_name", "")
# dbutils.widgets.text("workspace_id", "")
# dbutils.widgets.text("dataset_mapping", "")
# dbutils.widgets.text("env", "")

# COMMAND ----------

# DBTITLE 1,import base functions
# MAGIC %run ./base_functions

# COMMAND ----------

try:
    notebook_vars = [
        'client_id',
        'azure_key_vault',
        'secret_name',
        'tenant_name',
        'workspace_id',
        'dataset_mapping',
        'env'
    ]
    client_id, azure_key_vault, secret_name, tenant_name, workspace_id, dataset_mapping, env = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    raise Exception(f"Error: {str(e)}")

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

# DBTITLE 1,Extract usecase name and dataset names
import ast

# Convert string to dictionary
dataset_mapping = ast.literal_eval(dataset_mapping)

# Extract usecase and dataset names
usecase_name = next(iter(dataset_mapping))
dataset_names = dataset_mapping[usecase_name]

print("Usecase Name:", usecase_name)
print("Dataset Names:", dataset_names)


# COMMAND ----------

# DBTITLE 1,get_datasets_mapping
import requests

def get_datasets_mapping(tenant_name, client_id, client_secret, workspace_id, target_dataset_names, usecase_name):
    # Authenticate
    authority_url = f"https://login.microsoftonline.com/{tenant_name}"
    scope = ["https://analysis.windows.net/powerbi/api/.default"]

    #Use MSAL to grab token
    app = msal.ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
    result = app.acquire_token_for_client(scopes=scope)
    # print(result)
    access_token = result['access_token']

    # Fetch datasets from Power BI workspace
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to get datasets: {response.status_code} - {response.text}")

    datasets = response.json().get("value", [])

    # Filter datasets by names
    matched_datasets = [
        {"name": d["name"], "dataset_id": d["id"]}
        for d in datasets
        if d["name"] in target_dataset_names
    ]

    # Create final mapping structure
    datasets_mapping = {
        "usecase": usecase_name,
        "datasets": matched_datasets
    }

    return datasets_mapping


# COMMAND ----------

# DBTITLE 1,extract dataset ids and create datasets mapping
#Set parameters
client_secret =  dbutils.secrets.get(azure_key_vault, secret_name)

datasets_mapping = get_datasets_mapping(
    tenant_name,
    client_id,
    client_secret,
    workspace_id,
    dataset_names,
    usecase_name
)

print(datasets_mapping)

# COMMAND ----------

# DBTITLE 1,parse datasets mapping
import json

try:
    # Parse if string
    if isinstance(datasets_mapping, str):
        datasets_mapping = json.loads(datasets_mapping)

    # Extract dataset IDs
    dataset_ids = [d["dataset_id"] for d in datasets_mapping.get("datasets", [])]

    # print to verify
    if dataset_ids:
        print("Dataset IDs:", dataset_ids)
    else:
        print("No datasets found in the mapping.")

except (json.JSONDecodeError, TypeError, KeyError) as e:
    print(f"Error parsing datasets_mapping: {e}")


# COMMAND ----------

# DBTITLE 1,define scope, authority url
#Set parameters
client_secret =  dbutils.secrets.get(azure_key_vault, secret_name)
authority_url = f"https://login.microsoftonline.com/{tenant_name}"
scope = ["https://analysis.windows.net/powerbi/api/.default"]

# COMMAND ----------

# DBTITLE 1,generate auth token
#Use MSAL to grab token
app = msal.ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
result = app.acquire_token_for_client(scopes=scope)
# print(result)
access_token = result['access_token']

# COMMAND ----------

# DBTITLE 1,check refresh status
import requests

def check_status(dataset_id, workspace_id, access_token):
    if not dataset_id:
        print("No dataset ID provided. Skipping status check.")
        return "No dataset ID"

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error checking status for {dataset_id}: {e}") 
        return "Request failed"

    try:
        data = response.json()
    except ValueError:
        print(f"Invalid JSON response for dataset {dataset_id}: {response.text}")
        return "Invalid JSON response"

    # print(f"Refresh data for {dataset_id}: {data}")

    refreshes = data.get("value", [])
    if refreshes:
        return refreshes[0].get("status", "Unknown")
    else:
        return "No refresh history"


# COMMAND ----------

# DBTITLE 1,poll refresh status untill all completes
import time
import requests
import pandas as pd

# Handle empty list early
if not dataset_ids:
    print("No dataset IDs provided to check refresh status.")
else:
    # Initialize mapping
    dataset_status_map = {ds_id: "Unknown" for ds_id in dataset_ids}

    # Poll every 5 minutes until all statuses are resolved
    while "Unknown" in dataset_status_map.values():
        print("\nChecking refresh statuses...")
        for dataset_id in dataset_ids:
            if dataset_status_map[dataset_id] == "Unknown":
                status = check_status(dataset_id, workspace_id, access_token)
                dataset_status_map[dataset_id] = status
                print(f"{dataset_id} => {status}")

        if "Unknown" in dataset_status_map.values():
            print("Waiting 5 minutes before checking again...\n")
            time.sleep(300)  # Wait 5 minutes

    print("\nAll dataset refresh statuses retrieved:")
    print(dataset_status_map)


# COMMAND ----------

# DBTITLE 1,regenerate auth token
#Generating access_token second time, in case token expires while datasets refreshing is in progress
app = msal.ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
result = app.acquire_token_for_client(scopes=scope)
# print(result)
access_token = result['access_token']

# COMMAND ----------

# DBTITLE 1,Build refresh info DataFrame
from pyspark.sql.functions import col, expr
import pandas as pd
import requests
import json
from datetime import timezone
import pytz

def ensure_utc(timestamp_str):
    ts = pd.to_datetime(timestamp_str)
    if ts.tzinfo is None:
        return ts.tz_localize('UTC')
    else:
        return ts.tz_convert('UTC')


# Extract use case and dataset list
usecase = datasets_mapping.get("usecase")
datasets = datasets_mapping.get("datasets", [])

refresh_records = []

if not datasets:
    print("No datasets found to check refresh status.")
    refresh_df = None  # Ensures the variable always exists
else:
    for dataset in datasets:
        dataset_id = dataset["dataset_id"]
        report_name = dataset["name"]

        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            result = response.json().get('value', [])
            if result:
                refresh_info = result[0]
                status = refresh_info.get('status')
                error_message = None

                if status == 'Failed':
                    exception = refresh_info.get('serviceExceptionJson')
                    if exception:
                        try:
                            error_json = json.loads(exception) if isinstance(exception, str) else exception
                            error_message = error_json.get('message') or str(error_json)
                        except Exception:
                            error_message = str(exception)

                refresh_records.append({
                    'refresh_date': pd.Timestamp.today().date(),
                    'usecase': usecase,
                    'report_name': report_name,
                    'dataset_id': dataset_id,
                    'request_id': refresh_info.get('requestId'),
                    'refresh_type': refresh_info.get('refreshType'),
                    'start_time': ensure_utc(refresh_info.get('startTime')) if refresh_info.get('startTime') else None,
                    'end_time': ensure_utc(refresh_info.get('endTime')) if refresh_info.get('endTime') else None,
                    'refresh_duration': (
                        pd.to_datetime(refresh_info.get('endTime')) - 
                        pd.to_datetime(refresh_info.get('startTime'))
                    ).total_seconds() if refresh_info.get('startTime') and refresh_info.get('endTime') else None,
                    'status': status,
                    'error_message': error_message,
                    'log_timestamp': pd.Timestamp.now(tz='UTC')  # Already timezone-aware
                })

        else:
            print(f"Failed to fetch refresh info for {dataset_id}: HTTP {response.status_code} — {response.text}")

    # Create DataFrame
    if refresh_records:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
        schema = StructType([
            StructField('refresh_date', DateType(), True),
            StructField('usecase', StringType(), True),
            StructField('report_name', StringType(), True),
            StructField('dataset_id', StringType(), True),
            StructField('request_id', StringType(), True),
            StructField('refresh_type', StringType(), True),
            StructField('start_time', TimestampType(), True),
            StructField('end_time', TimestampType(), True),
            StructField('refresh_duration', FloatType(), True),
            StructField('status', StringType(), True),
            StructField('error_message', StringType(), True),
            StructField('log_timestamp', TimestampType(), True)
        ])
        df = spark.createDataFrame(refresh_records, schema=schema)


        df = df.withColumn(
            "refresh_duration_hms",
            expr("""
                CASE WHEN refresh_duration IS NOT NULL THEN
                    concat(
                        cast(floor(refresh_duration / 3600) as int), 'h ',
                        cast(floor((refresh_duration % 3600) / 60) as int), 'm ',
                        cast(floor(refresh_duration % 60) as int), 's'
                    )
                ELSE NULL END
            """)
        )

        print("\nFinal Refresh Status Table:")
        refresh_df = df.select(
            "refresh_date", "usecase", "report_name", "dataset_id", "request_id",
            "refresh_type", "start_time", "end_time", "refresh_duration",
            "refresh_duration_hms", "status", "error_message", "log_timestamp"
        )
        # display(refresh_df)
    else:
        print("No refresh records found.")


# COMMAND ----------

# DBTITLE 1,declare dpm_pbi_refresh_logs tables
catalog = f"transformation_{env}"
print(catalog)
schema = "etl_audit"
pbi_refresh_table = "dpm_pbi_refresh_logs"
pbi_refresh_log_table = f"{catalog}.{schema}.{pbi_refresh_table}"

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,write refresh logs into table
from delta.tables import DeltaTable

# Proceed only if DataFrame is not empty
if refresh_df and refresh_df.count() > 0:
    if spark.catalog.tableExists(pbi_refresh_log_table):
        delta_table = DeltaTable.forName(spark, pbi_refresh_log_table)

        delta_table.alias("target").merge(
            refresh_df.alias("source"),
            "target.dataset_id = source.dataset_id AND target.request_id = source.request_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

        print(f"Merge completed into {pbi_refresh_log_table}.")
    else:
        refresh_df.write.format("delta").mode("overwrite").saveAsTable(pbi_refresh_log_table)
        print(f"Table {pbi_refresh_log_table} created and data saved.")
else:
    print("No data in refresh_df. Skipping merge/write operation.")

