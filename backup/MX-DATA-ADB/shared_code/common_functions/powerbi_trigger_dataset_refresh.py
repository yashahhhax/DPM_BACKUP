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

# DBTITLE 1,import base functions
# MAGIC %run ./base_functions

# COMMAND ----------

# DBTITLE 1,import etl util functions for powerbi
# MAGIC %run ./etl_utils_powerbi

# COMMAND ----------

# DBTITLE 1,import etl util functions for teams notifications
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

# DBTITLE 1,install msal (Microsoft Authentication Library) library
# MAGIC %pip install msal

# COMMAND ----------

# DBTITLE 1,import required libraries, functions
import msal
import requests
import json
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Read required dataset details from dpm_pbi_datasets
import json

if modules:
    modules = json.loads(modules)

    # normalize modules to lowercase
    modules_list = "', '".join([m.lower() for m in modules])

    pbi_datasets_df = spark.sql(f"""
        SELECT *
        FROM transformation_{env}.config.pbi_datasets
        WHERE lower(usecase) = lower('{usecase}')
          AND lower(module) IN ('{modules_list}')
          AND refresh_required = 'Y'
    """)
else:
    pbi_datasets_df = spark.sql(f"""
        SELECT *
        FROM transformation_{env}.config.pbi_datasets
        WHERE lower(usecase) = lower('{usecase}')
          AND refresh_required = 'Y'
    """)


# COMMAND ----------

 display(pbi_datasets_df)

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

# DBTITLE 1,call trigger_dataset_refresh based on current refresh status
# Trigger refresh for all datasets
import pandas as pd

# Step 1: Extract list of (workspace_id, dataset_id) tuples
dataset_tuples = pbi_datasets_df.select("usecase", "workspace_id", "report_name", "dataset_id").distinct().toPandas()
dataset_tuples = list(dataset_tuples.itertuples(index=False, name=None))
print(dataset_tuples)

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

            if status == 'Unknown':
                print(f"Refresh status for {report_name} is unknown (refresh is in progress). Skipping...")
            else:
                # Trggering power bi dataset refresh
                trigger_dataset_refresh(workspace_id, dataset_id, access_token)
        else:
            print(f"No refresh information found for {report_name}. Skipping...")
    else:
        print(f"Error: {response.status_code} - {response.text}")

# COMMAND ----------

# DBTITLE 1,check_powerbi_refresh_status
refresh_df = check_powerbi_refresh_status(dataset_tuples, access_token)

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

# DBTITLE 1,merge_refresh_status_to_delta
# Merge into audit table (even if some refreshes failed or unknown)
merge_refresh_status_to_delta(refresh_df, pbi_refresh_log_table)
