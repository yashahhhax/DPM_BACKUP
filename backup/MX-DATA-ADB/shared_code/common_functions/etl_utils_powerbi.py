# Databricks notebook source
# DBTITLE 1,check_status
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

# DBTITLE 1,trigger_dataset_refresh
import requests

def trigger_dataset_refresh(workspace_id, dataset_id, access_token):
    if not dataset_id:
        print("No dataset IDs provided. Skipping dataset refresh.")
        return  # Exit early without raising an error

    errors = []

    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.post(url, headers=headers)

    if response.status_code == 202:
        print(f"Refresh triggered for dataset: {dataset_id}")
    else:
        error_msg = f"Failed to trigger refresh for {dataset_id} — HTTP {response.status_code}: {response.text}"
        print(error_msg)
        errors.append(error_msg)

    if errors:
        raise RuntimeError("One or more dataset refreshes failed:\n" + "\n".join(errors))


# COMMAND ----------

# DBTITLE 1,check_powerbi_refresh_status
def check_powerbi_refresh_status(dataset_tuples, access_token):
    from pyspark.sql.functions import col, expr
    import pandas as pd
    import requests
    import json
    from datetime import timezone, timedelta
    import pytz

    def add_uae_delta(timestamp_str):
        ts = pd.to_datetime(timestamp_str)
        ts_shifted = ts + timedelta(hours=4)
        return ts + timedelta(hours=4)

    refresh_records = []
    refresh_df = None  # Initialize to None by default

    if not dataset_tuples:
        print("No datasets found to check refresh status.")
    else:
        for usecase, workspace_id, report_name, dataset_id in dataset_tuples:
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
                        'workspace_id': workspace_id,
                        'report_name': report_name,
                        'dataset_id': dataset_id,
                        'request_id': refresh_info.get('requestId'),
                        'refresh_type': refresh_info.get('refreshType'),
                        'start_time': add_uae_delta(refresh_info.get('startTime')) if refresh_info.get('startTime') else None,
                        'end_time': add_uae_delta(refresh_info.get('endTime')) if refresh_info.get('endTime') else None,
                        'refresh_duration': (
                            pd.to_datetime(refresh_info.get('endTime')) - 
                            pd.to_datetime(refresh_info.get('startTime'))
                        ).total_seconds() if refresh_info.get('startTime') and refresh_info.get('endTime') else None,
                        'status': status,
                        'error_message': error_message,
                        'log_timestamp': pd.Timestamp.now(tz='UTC') + timedelta(hours=4)
                    })

            else:
                print(f"Failed to fetch refresh info for {dataset_id}: HTTP {response.status_code} — {response.text}")

        # Create DataFrame
        if refresh_records:
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
            schema = StructType([
                StructField('refresh_date', DateType(), True),
                StructField('usecase', StringType(), True),
                StructField('workspace_id', StringType(), True),
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

            refresh_df = df.select(
                "refresh_date", "usecase", "workspace_id", "report_name", "dataset_id", "request_id",
                "refresh_type", "start_time", "end_time", "refresh_duration",
                "refresh_duration_hms", "status", "error_message", "log_timestamp"
            )
            print("\nFinal Refresh Status Table:")
            # display(refresh_df)
        else:
            print("No refresh records found.")

    return refresh_df  # Final return

# COMMAND ----------

# DBTITLE 1,merge_refresh_status_to_delta
def merge_refresh_status_to_delta(refresh_df, pbi_refresh_log_table):
    from delta.tables import DeltaTable

    if refresh_df is not None and refresh_df.count() > 0:
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
