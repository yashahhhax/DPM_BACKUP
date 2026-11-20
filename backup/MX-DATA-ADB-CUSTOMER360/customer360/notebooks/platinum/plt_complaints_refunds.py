# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC
# MAGIC ### Pipeline Overview
# MAGIC
# MAGIC | Attribute        | Details                                              |
# MAGIC | ---------------- | ---------------------------------------------------- |
# MAGIC | **Table Name**   | complaints_refunds                                    |
# MAGIC | **Layer**        | Platinum                                             |
# MAGIC | **Target Table** | transformation_dev.plt_customer360.complaints_refund |
# MAGIC | **Domain**       | Customer 360 – Complaints & Refunds                  |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Source Details
# MAGIC
# MAGIC | Schema | Table                    | Purpose                                  |
# MAGIC | ------ | ------------------------ | ---------------------------------------- |
# MAGIC | crm    | complaints_refunds_source | Source – complaints and refund case data |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Load Strategy
# MAGIC
# MAGIC | Load Type            | Description                                                                                    |
# MAGIC | -------------------- | ---------------------------------------------------------------------------------------------- |
# MAGIC | **Full Load**        | Initial run (TRUNCATE + INSERT)                                                                |
# MAGIC | **Incremental Load** | MERGE on incident_id using modifiedon with 30-day backward buffer to handle late-arriving data |
# MAGIC | **Date Range Load**  | Filtered MERGE on modifiedon when start_date and end_date are provided                         |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Technical Details
# MAGIC
# MAGIC | Attribute              | Details                   |
# MAGIC | ---------------------- | ------------------------- |
# MAGIC | **Primary Key**        | incident_id               |
# MAGIC | **Business Key(s)**    | customer_id               |
# MAGIC | **Watermark Column**   | modifiedon                |
# MAGIC | **Buffer Days**        | 30                        |
# MAGIC | **Schema Enforcement** | Yes (pre-load validation) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Notes
# MAGIC
# MAGIC | Item               | Description                          |
# MAGIC | ------------------ | ------------------------------------ |
# MAGIC | Late Arriving Data | Handled using 30-day backward buffer |
# MAGIC | Idempotency        | Ensured via MERGE logic              |
# MAGIC | Data Freshness     | Controlled via modifiedon watermark  |
# MAGIC

# COMMAND ----------

from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import List
import pandas as pd
import numpy as np
import pyspark.sql as SQL
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import (col,lit,trim,expr,desc,rank,add_months,current_date,date_sub,from_json)
import pyspark.sql.types as T
from pyspark.sql.types import (StructType,StringType,MapType
)
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,Include Base Function
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %run ../../config/view_queries

# COMMAND ----------

# DBTITLE 1,define env
try:
    notebook_vars = [
        'env','start_date','end_date','current_job_run_id','buffer_days'
    ]
    env, start_date, end_date, current_job_run_id, buffer_days = get_notebook_params(*notebook_vars, verbose=True)
    buffer_days = int(buffer_days) if buffer_days is not None and buffer_days != '' else 30
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,remove_non_matches Function
def remove_non_matches(df_name:str, df:SQL.DataFrame, cols_to_match:List[str], pattern:str) -> SQL.DataFrame:
  """
  Description:
  Removes non-matching rows from a PySpark DataFrame based on a regular expression pattern.
  The filtering happens for all columns provided in the column list

  Parameters:
  df_name (str): The name of the DataFrame.
  df (pyspark.sql.DataFrame): The PySpark DataFrame to filter.
  cols_to_match (List[str]): A list of column names to apply the regular expression pattern to.
  pattern (str): A regular expression pattern to use for matching.

  Returns:
  pyspark.sql.DataFrame: A new PySpark DataFrame with rows removed that do not match the given pattern.

  Example:
  df = spark.read.csv('data.csv', header=True)
  filtered_df = remove_non_matches('my_df', df, ['col1', 'col2'], 'pattern')
  
  This will remove all rows from my_df dataframe where any of the specified columns don't match the regex pattern
  """
  # rows_before = df.count()
  for select_col in cols_to_match:
    df = df.filter(F.col(select_col).rlike(pattern))
  # rows_after = df.count()
  # print(f"{df_name}: Rows before: {rows_before}, Rows after: {rows_after}, Rows_removed:{rows_before-rows_after}")
  return df
  

# COMMAND ----------

# DBTITLE 1,make_sql_table_unique Function
# default filter means no filter as its always True (1=1)
def make_sql_table_unique(table:str, unique_col:str,
                          order_by_col1:str, order_by_col2:str= None,
                          ordering_col1:str= 'desc', ordering_col2:str= 'desc', 
                          filters:str= '1=1', overwrite_table:bool= True) -> SQL.DataFrame:
  """
  Description:
  Returns a DataFrame with unique values for a given column.

  Parameters:
  table (str): The SQL temp view name for the input table to be de-duplicated
  unique_col (str): The name of the column with the unique values.
  order_by_col1 (str): The name of the column to order by.
  order_by_col2 (str): The name of the second column to order by (default: None).
  ordering_col1 (str): The ordering of the first column (default: 'desc').
  ordering_col2 (str): The ordering of the second column (default: 'desc').
  filters (str): SQL WHERE clause with filtering conditions for the input table (default: '1=1' i.e. no filtering).
  overwrite_table (bool): If True, overwrite the input temp table view with the transformed unique data, otherwise create a new temp table view with suffix '_unique' (default: True).

  Returns:
  pyspark.sql.DataFrame: A DataFrame with unique values for specified column.

  Example:
  >>> input_df = spark.createDataFrame([(1, 'A', '2022-01-01'), (2, 'B', '2022-01-02'), 
                                        (3, 'A', '2022-01-03'), (4, 'C', '2022-01-04'), 
                                        (5, 'D', '2022-01-05'), (6, 'D', '2022-01-06'), 
                                        (7, 'A', '2022-01-07')], ['id', 'value', 'date'])
  >>> input_df.createOrReplaceTempView('sql_temp_view')
  >>> output_df = make_sql_table_unique('sql_temp_view', 'value', 'date', 'id')
  successfully created table view sql_temp_view and 3 duplicate rows removed (ordering:desc,desc, filters: 1=1)
  """
  
  query = f"""
    with a_data as
    (select *,
          row_number() OVER (PARTITION BY {unique_col} ORDER BY {order_by_col1} {ordering_col1}
                        {', '+order_by_col2 if order_by_col2 is not None else ''} {ordering_col2 if order_by_col2 is not None else ''}) as sno
    from {table}
    where {filters})

  select * except (sno)
  from a_data
  where sno=1"""
  
  previous_rows = spark.sql(f"select count(*) from {table} where {filters}").collect()[0][0]
  
  query_df= spark.sql(query)
  if overwrite_table==True:
    query_df.createOrReplaceTempView(f"{table}")  
    updated_rows = spark.sql(f"select count(*) from {table}").collect()[0][0]
    print(f"successfully replaced table view {table} and {previous_rows-updated_rows} duplicate rows removed (ordering:{ordering_col1}{','+ordering_col2 if order_by_col2 is not None else ''}, filters: {filters})")
  else:
    query_df.createOrReplaceTempView(f"{table}_unique")
    updated_rows = spark.sql(f"select count(*) from {table}_unique").collect()[0][0]
    print(f"successfully created table view {table}_unique and {previous_rows-updated_rows} duplicate rows removed (ordering:{ordering_col1}{','+ordering_col2 if order_by_col2 is not None else ''}, filters: {filters})")
  return query_df

# COMMAND ----------

# DBTITLE 1,GUID_REGEX_PATTERN
GUID_REGEX_PATTERN = '^' + '-'.join(['[a-zA-Z0-9]+']*5) + '$'
NAME_REGEX_PATTERN = '[a-zA-Z]+'   # should contain some letters (i.e. not only a number)

# COMMAND ----------

# DBTITLE 1,crm_schema
crm_schema = f"transformation_{env}.brz_crm"
tgt_db = f"transformation_{env}.plt_customer360"

# Source table names
vw_crm_incident = f"{crm_schema}.vw_crm_incident"  
vw_crm_stringmap = f"{crm_schema}.vw_crm_stringmap"
vw_crm_dot_property = f"{crm_schema}.vw_crm_dot_property"
vw_crm_contact = f"{crm_schema}.vw_crm_contact"

src_scv   = f"{tgt_db}.scv_account_master"

# Target table name
target_table = f"transformation_{env}.plt_customer360.complaints_and_refunds"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

# Test descriptions to exclude from complaints/refunds (maintain this list as new test cases appear)
TEST_DESCRIPTIONS_EXCLUSION = [
    'test','test from arabot','test comp','testingyww complaint usecase on the bot',
    'test complaint','abc test','sample case for testing portal','hi test',
    'this is a test','just a test ignore','test diala','test2',
    'test message. ignore.','just a test','farah production test 01',
    'this is a test please neglect','test test','testing handove',
    'hi this is a test','this is a test assign it to me',
    'i need to test the system','hello test','farah system testing','product test.'
]
# Format for SQL IN clause
test_descriptions_sql = ", ".join(f"'{desc}'" for desc in TEST_DESCRIPTIONS_EXCLUSION)

print(f"Source table: {vw_crm_incident}")
print(f"Source table: {vw_crm_stringmap}")
print(f"Source table: {vw_crm_dot_property}")
print(f"Source table: {vw_crm_contact}")
print(f"Target table: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")
print(f"Test exclusions: {len(TEST_DESCRIPTIONS_EXCLUSION)} patterns")

# COMMAND ----------

# DBTITLE 1,clean_crm_incident_return_df
def clean_crm_incident_return_df(source_table: str) -> DataFrame:
    """
    Cleans CRM table:
    - Removes duplicates (excluding adf_load_time)
    - Drops fully null rows
    - Trims all string columns
    """
    # 1. Read table
    df = spark.table(source_table)

    # 2. Identify columns (excluding adf_load_time)
    columns_to_consider = [c for c in df.columns if c != "adf_load_time"]

    # 3. Drop duplicates
    df = df.dropDuplicates(columns_to_consider)

    # 4. Drop rows where all columns are null
    df = df.dropna(how="all", subset=columns_to_consider)

    # 5. Trim all string columns in a single select (avoids long transformation chain)
    df = df.select([
        trim(col(c)).alias(c) if isinstance(f.dataType, StringType) else col(c)
        for c, f in zip(df.columns, df.schema.fields)
    ])

    return df

# COMMAND ----------

# DBTITLE 1,clean_and_return_df
clean_crm_incident = clean_crm_incident_return_df(vw_crm_incident)
clean_crm_stringmap = clean_and_return_df(vw_crm_stringmap)
clean_crm_dot_property = clean_and_return_df(vw_crm_dot_property)
clean_crm_contact = clean_and_return_df(vw_crm_contact)

# COMMAND ----------

# DBTITLE 1,remove_non_matches
# Matching pattern for account and contact across tables

df_crm_cases = remove_non_matches('vw_crm_incident', clean_crm_incident, ['customerid'], GUID_REGEX_PATTERN)
df_crm_contacts = remove_non_matches('vw_crm_contact', clean_crm_contact, ['contactid'], GUID_REGEX_PATTERN)

df_crm_stringmap = remove_non_matches('vw_crm_stringmap', clean_crm_stringmap, [], GUID_REGEX_PATTERN) #No pattern for string map data

df_crm_dot_property = remove_non_matches('vw_crm_dot_property', clean_crm_dot_property, ['dot_propertyid'], GUID_REGEX_PATTERN)
# df_crm_dot_property = remove_non_matches('vw_crm_dot_property', clean_crm_dot_property, ['dot_name'], NAME_REGEX_PATTERN)
df_crm_dot_property = remove_non_matches('vw_crm_dot_property', df_crm_dot_property, ['dot_name'], NAME_REGEX_PATTERN)

# COMMAND ----------

# DBTITLE 1,createOrReplaceTempView
df_crm_cases.createOrReplaceTempView('vw_crm_incident')
df_crm_contacts.createOrReplaceTempView('vw_crm_contact')
df_crm_stringmap.createOrReplaceTempView('vw_crm_stringmap')
df_crm_dot_property.createOrReplaceTempView('vw_crm_dot_property')

# COMMAND ----------

# DBTITLE 1,make_sql_table_unique
# De-duplication of tables

df_crm_contacts = make_sql_table_unique('vw_crm_contact', unique_col='contactid', order_by_col1='createdon', order_by_col2='modifiedon')

df_crm_stringmap = make_sql_table_unique("vw_crm_stringmap", unique_col="objecttypecode, attributename, attributevalue", order_by_col1="displayorder")

df_crm_dot_property = make_sql_table_unique('vw_crm_dot_property', unique_col='dot_propertyid', order_by_col1='createdon', order_by_col2='modifiedon')

# Not doing de-duplication of crm cases as thats the main table to which all other tables are left joined

# COMMAND ----------

# DBTITLE 1,Transformation and cleaning
df_crm_complaints_refunds = spark.sql(f"""

WITH 
-- String Maps
casetypecode_smap AS (
    SELECT * FROM vw_crm_stringmap
    WHERE objecttypecode='incident' AND attributename='casetypecode'
),
statuscode_smap AS (
    SELECT * FROM vw_crm_stringmap
    WHERE objecttypecode='incident' AND attributename='statuscode'
),
yc_paymentsource_smap AS (
    SELECT * FROM vw_crm_stringmap 
    WHERE objecttypecode='incident' AND attributename='yc_paymentsource'
),
yc_source_smap AS (
    SELECT * FROM vw_crm_stringmap 
    WHERE objecttypecode='incident' AND attributename='yc_source'
),
yc_reasonforrefund_smap AS (
    SELECT * FROM vw_crm_stringmap
    WHERE objecttypecode='incident' AND attributename='yc_reasonforrefund'
),
caseorigincode_smap AS (
    SELECT * FROM vw_crm_stringmap 
    WHERE objecttypecode='incident' AND attributename='caseorigincode'
),
prioritycode_smap AS (
    SELECT * FROM vw_crm_stringmap
    WHERE objecttypecode='incident' AND attributename='prioritycode'
),
servicestage_smap AS (
    SELECT * FROM vw_crm_stringmap
    WHERE objecttypecode='incident' AND attributename='servicestage'
),
statecode_smap AS (
    SELECT * FROM vw_crm_stringmap 
    WHERE objecttypecode='incident' AND attributename='statecode'
),
yc_fullorpartialrefund_smap AS (
    SELECT * FROM vw_crm_stringmap
    WHERE objecttypecode='incident' AND attributename='yc_fullorpartialrefund'
),

-- scv master
scv_active AS (
    SELECT email_id, scv_account_sk
    FROM (
        SELECT email_id, scv_account_sk,
               ROW_NUMBER() OVER (
                   PARTITION BY email_id
                   ORDER BY parent_scv_account_sk ASC NULLS FIRST, modified_date DESC
               ) AS rn
        FROM {src_scv}
        where  source_name = 't_mdm_guest'
          and email_id IS NOT NULL
    ) WHERE rn = 1
),


-- Incident Base
incident_base AS (
    SELECT
        cases.*,
        CASE
            WHEN con.emailaddress1 LIKE '%@%' THEN LOWER(con.emailaddress1)
            WHEN con.emailaddress2 LIKE '%@%' THEN LOWER(con.emailaddress2)
            WHEN con.emailaddress3 LIKE '%@%' THEN LOWER(con.emailaddress3)
            WHEN cases.emailaddress LIKE '%@%' THEN LOWER(cases.emailaddress)
            ELSE NULL
        END AS raw_email
    FROM vw_crm_incident cases
    LEFT JOIN vw_crm_contact con 
        ON con.contactid = cases.customerid
)


-- Final Layer
SELECT
    map('scv_sk',cast(scv.scv_account_sk as string),
       'omni_sk',NULL,
       'email_id', {email_standardization("raw_email")}
       ) as entity_identifiers,
    scv.scv_account_sk AS Owner_scv_sk, 
    CAST(NULL AS STRING) AS Owner_omni_sk,
    
    {email_standardization("raw_email")} as email_address,

    -- Keys
    incident_base.incidentid AS Incident_ID,

    -- Updated Watermark
    GREATEST(
        incident_base.modifiedon,
        incident_base.adx_resolutiondate,
        incident_base.yc_closewithcustomerdate,
        incident_base.yc_datecontacted,
        incident_base.yc_dateofcomplaint,
        incident_base.yc_dateofincident,
        incident_base.yc_datesenttoguest,
        incident_base.yc_followupdate,
        incident_base.yc_itemlostdate,
        incident_base.yc_refundrequesteddate
    ) AS modifiedon,

    incident_base.TicketNumber AS Case_Ticket_Number,

    yc_source_smap.value AS Case_Source,
    caseorigincode_smap.value AS Case_Origin,
    casetypecode_smap.value AS Case_Type,

    incident_base.customerid AS Customer_ID,

    --incident_base.raw_email AS email_address,

    -- Case Date Logic
    CASE 
        WHEN incident_base.casetypecode=1 AND incident_base.yc_dateofcomplaint IS NOT NULL THEN incident_base.yc_dateofcomplaint
        WHEN incident_base.casetypecode=1 AND incident_base.yc_dateofincident IS NOT NULL THEN incident_base.yc_dateofincident
        WHEN incident_base.casetypecode=1 THEN incident_base.createdon
        WHEN incident_base.casetypecode=4 AND incident_base.yc_refundrequesteddate IS NOT NULL THEN incident_base.yc_refundrequesteddate
        WHEN incident_base.casetypecode=4 AND incident_base.yc_dateofincident IS NOT NULL THEN incident_base.yc_dateofincident
        WHEN incident_base.casetypecode=4 THEN incident_base.createdon
        ELSE NULL 
    END AS Case_Date,

    CASE 
        WHEN incident_base.casetypecode=1 AND incident_base.yc_dateofcomplaint IS NOT NULL THEN 'dateofcomplaint'
        WHEN incident_base.casetypecode=1 AND incident_base.yc_dateofincident IS NOT NULL THEN 'dateofincident'
        WHEN incident_base.casetypecode=1 THEN 'createdon'
        WHEN incident_base.casetypecode=4 AND incident_base.yc_refundrequesteddate IS NOT NULL THEN 'refundrequesteddate'
        WHEN incident_base.casetypecode=4 AND incident_base.yc_dateofincident IS NOT NULL THEN 'dateofincident'
        WHEN incident_base.casetypecode=4 THEN 'createdon'
        ELSE NULL 
    END AS Case_Date_Source,

    incident_base.title AS Case_Title,
    incident_base.description AS Case_Description,

    property.dot_name AS Case_Facility_Name,

    statuscode_smap.value AS Case_Status,
    servicestage_smap.value AS Case_Service_Stage,

    CAST(incident_base.yc_requestedapprovalamount AS STRING) AS Case_Requested_Refund_Amount,
    yc_reasonforrefund_smap.value AS Case_Refund_Reason,
    yc_paymentsource_smap.value AS Case_Payment_Source,
    CAST(incident_base.yc_datecontacted AS STRING) AS Case_Date_Contacted,
    incident_base.yc_additionalnotes AS Case_Additional_Notes,
    incident_base.yc_correctiveaction AS Case_Corrective_Action,

    map(
        'Case_Reservation_BookingID', CAST(incident_base.yc_reservationnumberbookingid AS STRING),
        'Case_prioritycode', CAST(prioritycode_smap.value AS STRING),
        'Case_statecode', CAST(statecode_smap.value AS STRING),
        'Case_caseage', CAST(incident_base.caseage AS STRING),
        'Case_yc_closewithcustomer', CAST(incident_base.yc_closewithcustomer AS STRING),
        'Case_yc_closewithcustomerdate', CAST(incident_base.yc_closewithcustomerdate AS STRING),
        'Case_yc_fullorpartialrefund', CAST(yc_fullorpartialrefund_smap.value AS STRING),
        'Uncleaned_Mail', CAST(incident_base.raw_email AS STRING)
    ) AS Others,

    'brz_crm' as source_name

FROM incident_base
LEFT JOIN scv_active scv 
    ON incident_base.raw_email = scv.email_id

LEFT JOIN casetypecode_smap 
    ON casetypecode_smap.attributevalue = incident_base.casetypecode

LEFT JOIN statuscode_smap 
    ON statuscode_smap.attributevalue = incident_base.statuscode

LEFT JOIN yc_paymentsource_smap 
    ON yc_paymentsource_smap.attributevalue = incident_base.yc_paymentsource

LEFT JOIN yc_source_smap 
    ON yc_source_smap.attributevalue = incident_base.yc_source

LEFT JOIN yc_reasonforrefund_smap 
    ON yc_reasonforrefund_smap.attributevalue = incident_base.yc_reasonforrefund

LEFT JOIN caseorigincode_smap 
    ON caseorigincode_smap.attributevalue = incident_base.caseorigincode

LEFT JOIN prioritycode_smap 
    ON prioritycode_smap.attributevalue = incident_base.prioritycode

LEFT JOIN servicestage_smap 
    ON servicestage_smap.attributevalue = incident_base.servicestage

LEFT JOIN statecode_smap 
    ON statecode_smap.attributevalue = incident_base.statecode

LEFT JOIN yc_fullorpartialrefund_smap 
    ON yc_fullorpartialrefund_smap.attributevalue = incident_base.yc_fullorpartialrefund

LEFT JOIN vw_crm_dot_property property 
    ON property.dot_propertyid = incident_base.yc_leisurefacilityid

WHERE 
      incident_base.casetypecode IN (1, 4)
  AND (incident_base.statuscode != 6 OR incident_base.statuscode IS NULL)
  AND LOWER(TRIM(incident_base.description)) NOT IN ({test_descriptions_sql})

""")

# Add ETL timestamps via DataFrame API (avoids SQL injection risk from f-string interpolation)
df_crm_complaints_refunds = (df_crm_complaints_refunds
    .withColumn("etl_created_datetime", F.lit(current_uae_timestamp))
    .withColumn("etl_updated_datetime", F.lit(current_uae_timestamp))
)

# This will rename all columns in df_crm_complaints_refunds to lowercase.
df_crm_complaints_refunds = df_crm_complaints_refunds.toDF(
    *[c.lower() for c in df_crm_complaints_refunds.columns]
)

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_crm_complaints_refunds = apply_schema_casting(df_crm_complaints_refunds, crm_complaints_refunds_schema)

# COMMAND ----------

# DBTITLE 1,write to UC
# Build DDL string from schema dict
crm_complaints_refunds_ddl = ",\n".join(
    f"{col_name} {dtype}" for col_name, dtype in crm_complaints_refunds_schema.items()
)

# Buffer in days
# buffer_days = 30

try:
    ddl_struct = StructType.fromDDL(crm_complaints_refunds_ddl)

    # Check if table exists
    table_exists = spark.catalog.tableExists(target_table)

    # Get watermark
    if table_exists:
        watermark = spark.sql(
            f"SELECT MAX(modifiedon) FROM {target_table}"
        ).collect()[0][0]
    else:
        watermark = None

    # Load mode flags
    use_date_range = bool(start_date and end_date)
    is_full_load   = (not table_exists) or (watermark is None and not use_date_range)

    if not table_exists:
        print("Load mode  : FULL LOAD (table does not exist)")
    elif use_date_range:
        print(f"Load mode  : DATE RANGE ({start_date} to {end_date})")
    elif is_full_load:
        print("Load mode  : FULL LOAD")
    else:
        print(f"Load mode  : INCREMENTAL (watermark: {watermark}, buffer: {buffer_days} days)")

    # -------------------- metrics initialization --------------------
    inserted_count = 0
    updated_count  = 0

    # -------------------- FULL LOAD --------------------
    if is_full_load:

        if compare_schema_lenient(df_crm_complaints_refunds.schema, ddl_struct):
            print("Schema validation passed")

            # Atomic overwrite (replaces TRUNCATE + append to avoid data loss window)
            (
                df_crm_complaints_refunds.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target_table)
            )

            # Get inserted count from target table (avoids redundant full scan of source DataFrame)
            inserted_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
            updated_count  = 0

            print("Complaints Refund Table Full load complete.")

        else:
            raise Exception("Schema mismatch between source and target")

    # -------------------- DATE RANGE --------------------
    elif use_date_range:

        df_incremental = df_crm_complaints_refunds.filter(
            (F.col("modifiedon") >= F.lit(start_date)) &
            (F.col("modifiedon") <= F.lit(end_date))
        )

        if compare_schema_lenient(df_incremental.schema, ddl_struct):
            print("Schema validation passed")

            delta_tgt = DeltaTable.forName(spark, target_table)

            df_result = (
                delta_tgt.alias("tgt")
                .merge(
                    df_incremental.alias("src"),
                    "tgt.Incident_ID = src.Incident_ID"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            # Metrics from merge result (avoids extra pre-merge scan)
            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

            print(f"Complaints Refund Table Date range upsert complete. Inserted: {inserted_count}, Updated: {updated_count}")

        else:
            raise Exception("Schema mismatch between source and target")

    # -------------------- INCREMENTAL --------------------
    else:

        if watermark:
            buffered_watermark = spark.createDataFrame(
                [(watermark,)], ["wm"]
            ).select(
                F.date_sub(F.col("wm"), buffer_days).alias("wm")
            ).collect()[0]["wm"]
        else:
            buffered_watermark = None

        print(f"Buffered watermark ({buffer_days} days): {buffered_watermark}")

        if buffered_watermark:
            df_incremental = df_crm_complaints_refunds.filter(
                F.col("modifiedon") > F.lit(buffered_watermark)
            )
        else:
            df_incremental = df_crm_complaints_refunds

        if compare_schema_lenient(df_incremental.schema, ddl_struct):
            print("Schema validation passed")

            delta_tgt = DeltaTable.forName(spark, target_table)

            df_result = (
                delta_tgt.alias("tgt")
                .merge(
                    df_incremental.alias("src"),
                    "tgt.Incident_ID = src.Incident_ID"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            # Metrics from merge result (avoids extra pre-merge scan)
            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

            print(f"Incremental upsert complete. Inserted: {inserted_count}, Updated: {updated_count}")

        else:
            raise Exception("Schema mismatch between source and target")

    # -------------------- FINAL COUNT --------------------
    if spark.catalog.tableExists(target_table):
        raw_count = spark.sql(
            f"SELECT COUNT(*) FROM {target_table}"
        ).collect()[0][0]

        print("Complaints Refund Table Loaded...")
        print(f"Rows Inserted : {inserted_count}")
        print(f"Rows Updated  : {updated_count}")
        print(f"Total Records : {raw_count}")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

#view creation
catalog     = f"transformation_{env}"
schema      = "plt_customer360"
object_name = "vw_complaints_and_refunds"
select_query = c360_view_queries[object_name].format(catalog=catalog, schema=schema)
create_view_or_table(catalog=catalog, schema=schema, object_name=object_name, select_query=select_query)

# COMMAND ----------

# add last_updated tag with current timestamp to identify table last refreshed datetime.
add_last_updated_tag(target_table)

# COMMAND ----------

plt_stats = {
    "gld_table"                   : target_table,
    "dim_new_records_count"       : 0,
    "dim_updated_records_count"   : 0,
    "fact_inserted_records_count" : inserted_count,
    "fact_updated_records_count"  : updated_count
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)

# COMMAND ----------

trigger_databricks_job_by_name('workflow_orchestration')
