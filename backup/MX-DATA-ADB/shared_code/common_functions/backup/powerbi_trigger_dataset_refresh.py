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

# DBTITLE 1,define trigger_dataset_refresh
import requests

def trigger_dataset_refresh(workspace_id, dataset_ids, access_token):
    if not dataset_ids:
        print("No dataset IDs provided. Skipping dataset refresh.")
        return  # Exit early without raising an error

    errors = []

    for dataset_id in dataset_ids:
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

# DBTITLE 1,call trigger_dataset_refresh
# Trigger refresh for all datasets
trigger_dataset_refresh(workspace_id, dataset_ids, access_token)
