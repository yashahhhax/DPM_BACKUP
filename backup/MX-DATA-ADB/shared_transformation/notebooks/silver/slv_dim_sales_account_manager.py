# Databricks notebook source
# MAGIC %md
# MAGIC ### Import necessary libraries

# COMMAND ----------

# MAGIC %md
# MAGIC  "primary_key_cols": [
# MAGIC             "ACCOUNT_MANAGER_ID",
# MAGIC             "SEGMENT_MANAGER_ID",
# MAGIC             "PARTNER_CRM_ACCOUNT_ID",
# MAGIC             "ACTIVE_FLAG",
# MAGIC             "CRM_LAST_MODIFIED_DATE",
# MAGIC             "CRM_ACTIVE_FROM",
# MAGIC             "CRM_ACTIVE_TO"
# MAGIC         ]

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, sha2, concat_ws, current_timestamp,
    when, coalesce, lit, row_number, to_date,
    unix_timestamp, from_utc_timestamp, isnull, length
)
from pyspark.sql.window import Window
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enviornment parameter will be passed from the workflow

# COMMAND ----------

# dbutils.widgets.text("env","")

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contains the expected schemas for DRR Objects

# COMMAND ----------

# MAGIC %run "../../config/schema_config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contains all the base functions required

# COMMAND ----------

# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Base function call to receive the param values

# COMMAND ----------

try:
    notebook_vars = [
        'env'
    ]
    env= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")
# env='dev'
# print(env)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Source and Target Tables

# COMMAND ----------

# ---------------------- Parameters & Paths ----------------------
brz = f"transformation_{env}.brz_crm"
silver = f"transformation_{env}.slv_shared"
silver_table = f"{silver}.prep_dim_sales_account_manager"

# Source table paths
crm_accounts = f"{brz}.vw_crm_account"
crm_segment = f"{brz}.vw_crm_dot_segment"
crm_accountmanagershistory = f"{brz}.vw_crm_yc_accountmanagerhistory"
crm_buprofile = f"{brz}.vw_crm_dot_accountbusinessunitprofile"
crm_systemuser = f"{brz}.vw_crm_systemuser"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Function Definition: rename_columns_with_dots(df)

# COMMAND ----------


def clean_and_return_df(table_name: str):
    """
    Reads a table and renames all columns:
    replaces '.' with '_' in column names.
    """
    df = spark.table(table_name)
    for c in df.columns:
        if "." in c:
            df = df.withColumnRenamed(c, c.replace(".", "_"))
    return df


# COMMAND ----------

df_accounts = clean_and_return_df(crm_accounts)
df_segment = clean_and_return_df(crm_segment)
df_history = clean_and_return_df(crm_accountmanagershistory)
df_bu = clean_and_return_df(crm_buprofile)
df_systemuser = clean_and_return_df(crm_systemuser)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read and Clean Source Bronze Tables

# COMMAND ----------


# Select required columns and register as temp views
df_accounts = df_accounts.select("accountid", "accountnumber", "name", "telephone1", "yc_omniaccountid", "dot_subsegmentid", "emailaddress1", "dot_segmentid", "yc_isehdmc", "industrycode", "modifiedon", "statecode", "statuscode", "dot_country", "dot_region")
df_accounts.createOrReplaceTempView("accounts")

df_segment = df_segment.select("dot_segmentid", "statecode", "statuscode", "createdby", "ownerid", "modifiedon", "dot_name", "modifiedby", "dot_parentsegment", "createdon", "dot_team")
df_segment.createOrReplaceTempView("dot_segment")

df_history = df_history.select("yc_accountmanagerhistoryid", "yc_name", "createdon", "statuscode", "statecode", "yc_startdate", "modifiedon", "modifiedby", "yc_enddate", "createdby", "yc_businessunitprofileid", "yc_businessunitid", "yc_accountmanagerid", "yc_accountid")
df_history.createOrReplaceTempView("yc_accountmanagerhistory")

df_bu = df_bu.select("dot_accountbusinessunitprofileid", "dot_name", "createdon", "statecode", "dot_sendforapproval", "dot_primarysourcemarket",  "modifiedon", "modifiedby", "dot_iskeyaccount", "createdby", "dot_businessunitid", "dot_approverid", "dot_approvalstatus", "ownerid", "statuscode", "dot_accountcategory", "dot_accountid","dot_businessunitidname")
df_bu.createOrReplaceTempView("dot_accountbusinessunitprofile")

df_systemuser = df_systemuser.select("systemuserid", "fullname", "businessunitid", "domainname", "islicensed", "setupuser", "emailrouteraccessapproval", "internalemailaddress", "preferredphonecode", "preferredemailcode", "preferredaddresscode", "organizationid", "modifiedon", "modifiedby", "defaultmailbox", "caltype", "lastname", "flpm_issponsorshipmanager", "dot_isaccountmanager", "invitestatuscode", "incomingemaildeliverymethod", "msdyn_gdproptout", "firstname", "deletedstate", "defaultodbfoldername",  "createdon", "createdby", "azurestate", "azuredeletedon", "azureactivedirectoryobjectid", "applicationid", "accessmode")
df_systemuser.createOrReplaceTempView("systemuser")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create staging tables

# COMMAND ----------

# Create stnd_crm_accounts
query_accounts = """
SELECT 
    accountid,
    accountnumber,
    name,
    NULL as _parentaccountid_value,  -- Not available in selected columns
    telephone1,
    NULL as _fe_omnidmgcategory_value,  -- Not available in selected columns
    yc_omniaccountid,
    dot_subsegmentid as _dot_subsegmentid_value,
    emailaddress1,
    dot_segmentid as _dot_segmentid_value,
    "" as dot_segment,
    "" as dot_subsegment,
    yc_isehdmc,
    industrycode,
    modifiedon,
    statecode,
    statuscode,
    dot_country as _dot_country_value,
    dot_region as _dot_region_value,
    current_timestamp() as LoadDate 
FROM accounts
"""
df_stnd_accounts = spark.sql(query_accounts)
df_stnd_accounts.createOrReplaceTempView("stnd_crm_accounts")

# Create stnd_crm_dot_segments
query_segments = """
SELECT 
    dot_segmentid,
    statecode,
    statuscode,
    createdby as _createdby_value,
    ownerid as _ownerid_value,
    modifiedon,
    dot_name,
    modifiedby as _modifiedby_value,
    dot_parentsegment as _dot_parentsegment_value,
    createdon,
    createdby as _createdonbehalfby_value,  -- Using createdby since it's available
    dot_team as _dot_team_value,
    modifiedby as _modifiedonbehalfby_value,  -- Using modifiedby since it's available
    current_timestamp() as LoadDate 
FROM dot_segment
"""
df_stnd_segments = spark.sql(query_segments)
df_stnd_segments.createOrReplaceTempView("stnd_crm_dot_segments")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Mapping_dwh_out6 table

# COMMAND ----------

# Create Mapping_dwh_out6
query_mapping = """
SELECT 
    row3.accountid as ACCOUNTID,
    row3.accountnumber as CRM_ACCOUNT_NUMBER,
    row3.name as CRM_ACCOUNT_NAME,
    row5.yc_accountid as MHIS_ACCOUNTID,
    row5.yc_accountmanagerid as ACCOUNT_MANAGER_ID,
    row5.yc_accountmanagerhistoryid as ACCOUNT_MANAGER_HIST_ID,
    row5.yc_businessunitid as BUSINESSUNIT_ID,
    to_date(row5.yc_startdate) as ACTIVE_FROM,
    to_date(row5.yc_enddate) as ACTIVE_TO,
    row5.statecode as HIST_STATE_CODE,
    row5.statuscode as HIST_STATUS_CODE,
    CASE 
        WHEN ISNULL(row5.statuscode) THEN "InActive" 
        WHEN row5.statuscode = 1 THEN "Active" 
        ELSE "InActive" 
    END as HIST_STATUS,
    row3.dot_segmentid as SEGMENT_ID 
FROM accounts row3 
LEFT OUTER JOIN yc_accountmanagerhistory row5 
    ON row3.accountid = row5.yc_accountid
"""
df_mapping = spark.sql(query_mapping)
df_mapping.createOrReplaceTempView("Mapping_dwh_out6")

# Create logers table (used in production but not referenced later)
query_logers = """
SELECT 
    row5.yc_accountmanagerid as ACCOUNT_MANAGER_ID,
    CASE 
        WHEN ISNULL(row5.yc_startdate) or length(row5.yc_startdate) = 0 
        THEN null 
        ELSE from_utc_timestamp(row5.yc_startdate, "Asia/Dubai") 
    END as Conv_From,
    row3.name as CRM_ACCOUNT_NAME 
FROM accounts row3 
LEFT OUTER JOIN yc_accountmanagerhistory row5 
    ON row3.accountid = row5.yc_accountid
"""
df_logers = spark.sql(query_logers)
df_logers.createOrReplaceTempView("logers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Result_1 table

# COMMAND ----------

# Create Result_1
query_result1 = """
SELECT 
    out6.ACCOUNTID as ACCOUNTID,
    out6.CRM_ACCOUNT_NUMBER as CRM_ACCOUNT_NUMBER,
    out6.CRM_ACCOUNT_NAME as CRM_ACCOUNT_NAME,
    out6.MHIS_ACCOUNTID as MHIS_ACCOUNTID,
    out6.ACCOUNT_MANAGER_ID as ACCOUNT_MANAGER_ID,
    out6.ACCOUNT_MANAGER_HIST_ID as ACCOUNT_MANAGER_HIST_ID,
    row6.fullname as ACCOUNT_MANAGER_NAME,
    out6.ACTIVE_FROM as ACTIVE_FROM,
    out6.ACTIVE_TO as ACTIVE_TO,
    out6.BUSINESSUNIT_ID as BUSINESSUNIT_ID,
    out6.HIST_STATE_CODE as HIST_STATE_CODE,
    out6.HIST_STATUS_CODE as HIST_STATUS_CODE,
    out6.HIST_STATUS as HIST_STATUS,
    out6.SEGMENT_ID as SEGMENT_ID,
    row9.dot_name as SEGMENT_NAME,
    row9.ownerid as SEGMENT_MANAGER_ID 
FROM Mapping_dwh_out6 out6 
LEFT OUTER JOIN (
    SELECT 
        row6.fullname,
        row6.systemuserid,
        row_number() OVER(PARTITION BY row6.systemuserid ORDER BY row6.systemuserid DESC) as TT  
    FROM systemuser row6
) row6 ON out6.ACCOUNT_MANAGER_ID = row6.systemuserid AND row6.TT = 1
LEFT OUTER JOIN (
    SELECT 
        row9.dot_name,
        row9.ownerid,
        row9.dot_segmentid,
        row_number() OVER(PARTITION BY row9.dot_segmentid ORDER BY row9.dot_segmentid DESC) as TT  
    FROM dot_segment row9
) row9 ON out6.SEGMENT_ID = row9.dot_segmentid AND row9.TT = 1
"""
df_result1 = spark.sql(query_result1)
df_result1.createOrReplaceTempView("Result_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Result_2 table

# COMMAND ----------

# Create Result_2
query_result2 = """
SELECT 
    Result_1.ACCOUNTID as ACCOUNTID,
    Result_1.CRM_ACCOUNT_NUMBER as CRM_ACCOUNT_NUMBER,
    Result_1.CRM_ACCOUNT_NAME as CRM_ACCOUNT_NAME,
    Result_1.MHIS_ACCOUNTID as MHIS_ACCOUNTID,
    Result_1.ACCOUNT_MANAGER_ID as ACCOUNT_MANAGER_ID,
    Result_1.ACCOUNT_MANAGER_HIST_ID as ACCOUNT_MANAGER_HIST_ID,
    Result_1.ACCOUNT_MANAGER_NAME as ACCOUNT_MANAGER_NAME,
    Result_1.ACTIVE_FROM as ACTIVE_FROM,
    Result_1.ACTIVE_TO as ACTIVE_TO,
    Result_1.BUSINESSUNIT_ID as BUSINESSUNIT_ID,
    row2.dot_businessunitidname as BUSINESSUNIT_NAME,
    Result_1.HIST_STATE_CODE as HIST_STATE_CODE,
    Result_1.HIST_STATUS_CODE as HIST_STATUS_CODE,
    Result_1.HIST_STATUS as HIST_STATUS,
    Result_1.SEGMENT_ID as SEGMENT_ID,
    Result_1.SEGMENT_NAME as SEGMENT_NAME,
    Result_1.SEGMENT_MANAGER_ID as SEGMENT_MANAGER_ID,
    row1.fullname as SEGMENT_MANAGER_NAME 
FROM Result_1 
LEFT OUTER JOIN (
    SELECT 
        row1.fullname,
        row1.systemuserid,
        row_number() OVER (PARTITION BY row1.systemuserid ORDER BY row1.systemuserid DESC) as TT  
    FROM systemuser row1
) row1 ON Result_1.SEGMENT_MANAGER_ID = row1.systemuserid AND row1.TT = 1
LEFT OUTER JOIN (
    SELECT 
        row2.dot_businessunitid,
        row2.dot_businessunitidname,
        row_number() OVER (PARTITION BY row2.dot_businessunitid ORDER BY row2.modifiedon DESC) as TT 
    FROM dot_accountbusinessunitprofile row2
) row2 ON Result_1.BUSINESSUNIT_ID = row2.dot_businessunitid AND row2.TT = 1
"""
df_result2 = spark.sql(query_result2)
df_result2.createOrReplaceTempView("Result_2")

# Create out table (used in production but not referenced in final query)
query_out = """
SELECT 
    Result_1.BUSINESSUNIT_ID as Parent_BU,
    row1.businessunitid as Child_BU,
    row2.dot_name as name 
FROM Result_1 
LEFT OUTER JOIN systemuser row1 ON Result_1.SEGMENT_MANAGER_ID = row1.systemuserid 
LEFT OUTER JOIN dot_segment row2 ON Result_1.BUSINESSUNIT_ID = row2.dot_segmentid
"""
df_out = spark.sql(query_out)
df_out.createOrReplaceTempView("out")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Transformations

# COMMAND ----------

# Final query with all transformations
query_final = """
SELECT 
    CASE WHEN ACCOUNT_MANAGER_ID='' THEN NULL ELSE ACCOUNT_MANAGER_ID END AS ACCOUNT_MANAGER_ID,
    CASE WHEN ACCOUNT_MANAGER_NAME='' THEN NULL ELSE ACCOUNT_MANAGER_NAME END AS ACCOUNT_MANAGER_NAME,
    CASE WHEN SEGMENT_MANAGER_ID='' THEN NULL ELSE SEGMENT_MANAGER_ID END AS SEGMENT_MANAGER_ID,
    CASE WHEN SEGMENT_MANAGER_NAME='' THEN NULL ELSE SEGMENT_MANAGER_NAME END AS SEGMENT_MANAGER_NAME,
    CASE WHEN SEGMENT_NAME='' THEN NULL ELSE SEGMENT_NAME END AS SEGMENT_NAME,
    CASE WHEN COMPANY_CODE='' THEN NULL ELSE COMPANY_CODE END AS COMPANY_CODE,
    CASE WHEN COMPANY_NAME='' THEN NULL ELSE COMPANY_NAME END AS COMPANY_NAME,
    CASE WHEN PARTNER_CRM_ACCOUNT_ID='' THEN NULL ELSE PARTNER_CRM_ACCOUNT_ID END AS PARTNER_CRM_ACCOUNT_ID,
    CASE WHEN BUSINESS_UNIT_ID='' THEN NULL ELSE BUSINESS_UNIT_ID END AS BUSINESS_UNIT_ID,
    CASE WHEN BUSINESS_UNIT_NAME='' THEN NULL ELSE BUSINESS_UNIT_NAME END AS BUSINESS_UNIT_NAME,
    CASE WHEN ACTIVE_FLAG='' THEN NULL ELSE ACTIVE_FLAG END AS ACTIVE_FLAG,
    CASE WHEN CRM_LAST_MODIFIED_DATE='' THEN NULL ELSE CRM_LAST_MODIFIED_DATE END AS CRM_LAST_MODIFIED_DATE,
    CASE WHEN CRM_ACTIVE_FROM='' THEN NULL ELSE CRM_ACTIVE_FROM END AS CRM_ACTIVE_FROM,
    CASE WHEN CRM_ACTIVE_TO='' THEN NULL ELSE CRM_ACTIVE_TO END AS CRM_ACTIVE_TO
FROM (
    SELECT 
        ACCOUNT_MANAGER_ID,
        ACCOUNT_MANAGER_NAME,
        SEGMENT_MANAGER_ID,
        SEGMENT_MANAGER_NAME,
        SEGMENT_NAME,
        COMPANY_CODE,
        COMPANY_NAME,
        PARTNER_CRM_ACCOUNT_ID,
        BUSINESS_UNIT_ID,
        BUSINESS_UNIT_NAME,
        ACTIVE_FLAG,
        CRM_LAST_MODIFIED_DATE,
        CRM_ACTIVE_FROM,
        CRM_ACTIVE_TO 
    FROM (
        SELECT 
            ACCOUNT_MANAGER_ID,
            ACCOUNT_MANAGER_NAME,
            SEGMENT_MANAGER_ID,
            SEGMENT_MANAGER_NAME,
            SEGMENT_NAME,
            COMPANY_CODE,
            COMPANY_NAME,
            PARTNER_CRM_ACCOUNT_ID,
            BUSINESS_UNIT_ID,
            BUSINESS_UNIT_NAME,
            ACTIVE_FLAG,
            CRM_LAST_MODIFIED_DATE,
            CRM_ACTIVE_FROM,
            CRM_ACTIVE_TO,
            row_number() OVER (
                PARTITION BY ACCOUNT_MANAGER_ID, SEGMENT_MANAGER_ID, PARTNER_CRM_ACCOUNT_ID, 
                            ACTIVE_FLAG, CRM_LAST_MODIFIED_DATE, CRM_ACTIVE_FROM, CRM_ACTIVE_TO 
                ORDER BY ACCOUNT_MANAGER_ID, SEGMENT_MANAGER_ID, PARTNER_CRM_ACCOUNT_ID, 
                         ACTIVE_FLAG, CRM_LAST_MODIFIED_DATE, CRM_ACTIVE_FROM, CRM_ACTIVE_TO
            ) rn 
        FROM (
            SELECT DISTINCT 
                row7.ACCOUNT_MANAGER_ID as ACCOUNT_MANAGER_ID,
                row7.ACCOUNT_MANAGER_NAME as ACCOUNT_MANAGER_NAME,
                row7.SEGMENT_MANAGER_ID as SEGMENT_MANAGER_ID,
                row7.SEGMENT_MANAGER_NAME as SEGMENT_MANAGER_NAME,
                row7.SEGMENT_NAME as SEGMENT_NAME,
                row7.CRM_ACCOUNT_NUMBER as COMPANY_CODE,
                row7.CRM_ACCOUNT_NAME as COMPANY_NAME,
                row7.ACCOUNTID as PARTNER_CRM_ACCOUNT_ID,
                row7.BUSINESSUNIT_ID as BUSINESS_UNIT_ID,
                row7.BUSINESSUNIT_NAME as BUSINESS_UNIT_NAME,
                row7.HIST_STATUS as ACTIVE_FLAG,
                cast(NULL as timestamp) as CRM_LAST_MODIFIED_DATE,
                cast(row7.ACTIVE_FROM as Timestamp) as CRM_ACTIVE_FROM,
                cast(row7.ACTIVE_TO as Timestamp) as CRM_ACTIVE_TO
            FROM Result_2 row7
        ) A
    ) A 
    WHERE rn = 1
)
"""
df_final = spark.sql(query_final)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up column names to match expected schema

# COMMAND ----------

# Standardize column names
def clean_col(col_name):
    cleaned = re.sub(r"[.\s]+", "_", col_name)         # Replace dots/spaces with _
    cleaned = re.sub(r"[^\w]", "_", cleaned)          # Replace other special chars
    cleaned = re.sub(r"__+", "_", cleaned)            # Collapse multiple underscores
    cleaned = cleaned.strip("_")                      # Trim leading/trailing _
    return cleaned.upper()

new_col_names = [clean_col(c) for c in df_final.columns]
df_final = df_final.toDF(*new_col_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing Duplicates

# COMMAND ----------

df_final = df_final.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Schema Casting

# COMMAND ----------

from pyspark.sql.functions import col, when

df_final = df_final.select([
    when(col(c) == '', None).otherwise(col(c)).alias(c)
    for c in df_final.columns
])


df_final = apply_schema_casting(df_final, dim_sales_account_manager_schema)

# COMMAND ----------

df_final=df_final.select(
    "PARTNER_CRM_ACCOUNT_ID",
    "BUSINESS_UNIT_ID",
    "ACTIVE_FLAG",
    "CRM_ACTIVE_FROM",
    "CRM_ACTIVE_TO",
    "ACCOUNT_MANAGER_ID",
    "SEGMENT_MANAGER_ID",
    "CRM_LAST_MODIFIED_DATE",
    "ACCOUNT_MANAGER_NAME",
    "SEGMENT_MANAGER_NAME",
    "COMPANY_CODE",
    "COMPANY_NAME",
    "BUSINESS_UNIT_NAME",
    "SEGMENT_NAME"
)


# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = [
    "ACCOUNT_MANAGER_ID",
    "SEGMENT_MANAGER_ID",
    "PARTNER_CRM_ACCOUNT_ID",
    "ACTIVE_FLAG",
    "CRM_LAST_MODIFIED_DATE",
    "CRM_ACTIVE_FROM",
    "CRM_ACTIVE_TO"
]

# Find duplicates
duplicate_df = (
    df_final
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in df_final based on keys {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in df_final.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Schema and Write Final DataFrame to Sales Account Manager Silver Table

# COMMAND ----------

# Build DDL string from schema dict
dim_sales_account_manager_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_sales_account_manager_schema.items()
)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_sales_account_manager_schema_ddl)
    if compare_schema_lenient(df_final.schema, ddl_struct):
        print("Schema matches!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_final.schema}</pre>")
        df_final.write.format("delta")\
                        .mode("overwrite")\
                        .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_final.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e
