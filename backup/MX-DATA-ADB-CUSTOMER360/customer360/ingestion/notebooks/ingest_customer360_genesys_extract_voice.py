# Databricks notebook source
# DBTITLE 1,Dependencies Initialization
import requests
import re
import base64
import json
import time
import pandas as pd
from pyspark.sql.functions import explode, col, lit, DataFrame, from_json, schema_of_json
from pyspark.sql.types import StructType, StringType, StructField
from datetime import datetime,date,timedelta
from zoneinfo import ZoneInfo
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import pyspark.sql as SQL

# COMMAND ----------

# DBTITLE 1,Pipeline Execution Parameters
dbutils.widgets.text('start_date', '')
dbutils.widgets.text('end_date', '')
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("datastore_name", "genesys")
dbutils.widgets.text("dataset_name", "customer360")
dbutils.widgets.text("write_mode", "append")
dbutils.widgets.text("secret_name", "cloud-genesys-api-prd-apikey")

# COMMAND ----------

# DBTITLE 1,Parameter Retrieval & Date Initialization
start_date = dbutils.widgets.get('start_date')
end_date = dbutils.widgets.get('end_date')
env = dbutils.widgets.get("env")
datastore_name = dbutils.widgets.get("datastore_name")
dataset_name = dbutils.widgets.get("dataset_name")
write_mode = dbutils.widgets.get("write_mode")
secret_name = dbutils.widgets.get("secret_name")
root_directory = f"abfss://ingestion@stmxdatadatalake{env}01.dfs.core.windows.net"
adf_load_time = datetime.now(ZoneInfo("Asia/Dubai"))

# COMMAND ----------

# DBTITLE 1,Date Validation
# Create schema if not existed already
spark.sql(f"CREATE SCHEMA IF NOT EXISTS ingestion_{env}.lnd_{datastore_name} MANAGED LOCATION '{root_directory}/landing/lnd_{datastore_name}'")

table_name = "voice_conversations"
target_table = f"ingestion_{env}.lnd_{datastore_name}.{dataset_name}_{table_name}"

# Current date
current_date = date.today().strftime("%Y-%m-%d")

# Handle widget values (None or empty)
if start_date not in (None, ""):
    start_date = start_date.strip()
else:
    start_date = ""

if end_date not in (None, ""):
    end_date = end_date.strip()
else:
    end_date = ""

# If both dates are passed in widget, use them directly
if start_date and end_date:
    final_start_date = start_date
    final_end_date = end_date

# Else derive dates based on target table
else:
    # If target table exists
    if spark.catalog.tableExists(target_table):
        max_segment_start = (
            spark.table(target_table)
            .select(F.max(F.to_date("segmentstart")).alias("max_segment_start"))
            .collect()[0]["max_segment_start"]
        )

        if max_segment_start:
            final_start_date = (max_segment_start - timedelta(days=7)).strftime("%Y-%m-%d")
            final_end_date = current_date
        else:
            # Table exists but no data
            final_start_date = "2026-01-01"
            final_end_date = current_date
    else:
        # Table does not exist
        final_start_date = "2026-01-01"
        final_end_date = current_date

# Convert to API datetime format
start_datetime = final_start_date + "T00:00:00.000Z"
end_datetime = final_end_date + "T23:59:59.999Z"

print(f"start_date      = {final_start_date}")
print(f"end_date        = {final_end_date}")
print(f"start_datetime  = {start_datetime}")
print(f"end_datetime    = {end_datetime}")

# COMMAND ----------

def build_clean_attributes_struct(attr_col):
    exprs = []
    for f in attr_field_names:
        clean_name = rename_map.get(f, f)
        # IMPORTANT: use getField() so spaces/dots are treated as literal field names
        exprs.append(attr_col.getField(f).alias(clean_name))
    return F.struct(*exprs)

def prepare_writing_dataframe(df: SQL.DataFrame) -> SQL.DataFrame:
    df = df.toDF(*(c.lower() for c in df.columns))
    df = ( df.withColumn("adf_load_time", F.lit(adf_load_time.isoformat())) )
    return df.drop(*[c for c in df.columns if c.startswith("_")])

def write_increment(df: SQL.DataFrame, merge_clause: str, target_table: str) -> None:
    df = df.toDF(*(c.lower() for c in df.columns))

    if spark.catalog.tableExists(target_table):
        dt = DeltaTable.forName(spark, target_table)

        print(f"Table exists with {dt.toDF().count()} → upserting {df.count()}")

        (
            dt.alias("s")
            .merge(df.alias("u"), merge_clause)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"Merged into {target_table}, now {dt.toDF().count()} rows")
    else:
        (
            df.write
            .format("delta")
            .mode("error")
            .saveAsTable(target_table)
        )
        print(f"Created table {target_table}")

# Deduplicate dataframe using row_number

def make_df_unique(df, unique_col, order_by_col1, order_by_col2=None):
    
    if order_by_col2:
        window_spec = Window.partitionBy(unique_col).orderBy(
            F.col(order_by_col1).desc(),
            F.col(order_by_col2).desc()
        )
    else:
        window_spec = Window.partitionBy(unique_col).orderBy(
            F.col(order_by_col1).desc()
        )
    
    df = (
        df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter("rn = 1")
        .drop("rn")
    )
    
    return df
    
def clean_output_col(name):
    return name.replace('`', '').replace('.', '_')

def normalize_key(key):
    return key.strip('`')

def build_input_col(struct_col, raw_key):
    if raw_key.startswith("`") and raw_key.endswith("`"):
        return F.col(f"`{struct_col}`.{raw_key}")
    else:
        return F.col(f"`{struct_col}`.`{raw_key}`")

# COMMAND ----------

client_id = '062ebeb7-a9e8-4c81-9fb3-16c381164fa8'
client_secret = dbutils.secrets.get(
    scope=f'kv-mx-data-adf-inges-{env}',
    key= secret_name
)
credentials = f"{client_id}:{client_secret}"
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
adf_load_time = datetime.now(ZoneInfo("Asia/Dubai"))

# COMMAND ----------

# DBTITLE 1,Secure Token Request via Client Credentials
url = "https://login.mypurecloud.de/oauth/token"
payload = "grant_type=client_credentials"
headers = {
    'Authorization': f'Basic {encoded_credentials}',
    'Content-Type': 'application/x-www-form-urlencoded',
}

response = requests.request("POST", url, data=payload, headers=headers)

if response.status_code == 200:
    json_data = response.json()
    access_token = json_data.get('access_token')
    if access_token:
        print("Access token:", access_token)
    else:
        print("Access token not found in the response JSON.")
else:
    print("Failed to get the response from the server. Status code:", response.status_code, "Response:", response.text)


# COMMAND ----------

# DBTITLE 1,Conversations Analytics Job Initialization
url = "https://api.mypurecloud.de/api/v2/analytics/conversations/details/jobs"

request_time_interval = start_datetime + '/' + end_datetime
params = {
    "interval": request_time_interval,
    "order": "asc",
    "orderBy": "conversationStart",
    "paging": {
        "pageSize": 1000,
        "pageNumber": 1000
    },
    "segmentFilters": [
        {
            "type": "or",
            "predicates": [
                {
                    "type": "dimension",
                    "dimension": "mediaType",
                    "operator": "matches",
                    "value": "voice"
                }
            ]
        }
    ],
    "conversationFilters": [
        {
            "type": "or",
            "predicates": [
                {
                    "type": "dimension",
                    "dimension": "originatingDirection",
                    "operator": "matches",
                    "value": "inbound"
                }
            ]
        }
    ]
}

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json' 
}

response = requests.post(url, headers=headers, json=params)

# Print the response text
print(response.text)
data = response.json()
job_id = data["jobId"]

# Print jobId 
print(f"Job ID: {job_id}")

# COMMAND ----------

# DBTITLE 1,Wait for Conversation Analytics Job Completion
url = f"https://api.mypurecloud.de/api/v2/analytics/conversations/details/jobs/{job_id}"
headers = {
    'Authorization': f'Bearer {access_token}'
}

response = requests.get(url, headers=headers, params=params)
data = response.json()

while data['state'] != 'FULFILLED':
  print("Job Status:",str(data))
  time.sleep(10)
  response = requests.get(url, headers=headers, params=params)
  data = response.json()

print("Job Status:",str(data))

# COMMAND ----------

# DBTITLE 1,Fetch Analytics Job Results Endpoint
url = f"https://api.mypurecloud.de/api/v2/analytics/conversations/details/jobs/{job_id}/results"
headers = {
    'Authorization': f'Bearer {access_token}'
}

# COMMAND ----------

# DBTITLE 1,Paginated API Extraction & Structured Data Processing
spark = SparkSession.builder.getOrCreate()

url = f"https://api.mypurecloud.de/api/v2/analytics/conversations/details/jobs/{job_id}/results"
headers = {"Authorization": f"Bearer {access_token}"}

has_more = True
cursor = None
payloads = []

while has_more:
    params = {"cursor": cursor} if cursor else {}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()

    payloads.append(json.dumps(data))
    cursor = data.get("cursor")
    has_more = bool(cursor)

raw_df = spark.createDataFrame(payloads, "string").toDF("json")

# Infer schema from first JSON safely
json_schema = spark.range(1).select(
    schema_of_json(lit(payloads[0]))
).collect()[0][0]

parsed_df = raw_df.select(from_json(col("json"), json_schema).alias("data"))

exploded_df = parsed_df.select(
    col("data.dataAvailabilityDate"),
    explode(col("data.conversations")).alias("conversation")
)

count_of_records = exploded_df.count()
dataAvailabilityDate = exploded_df.select("dataAvailabilityDate").first()[0]

print("Last day of data availability:", dataAvailabilityDate)
print("Count of records extracted:", count_of_records)


# COMMAND ----------

# DBTITLE 1,Exit Notebook If No Incremental Data
if str(count_of_records) == '0' :
  dbutils.notebook.exit("No incremental data available")

# COMMAND ----------

# DBTITLE 1,Select Required Conversation Fields (Schema Safe Handling)

selected_df = exploded_df.select("conversation.conversationId","conversation.conversationStart", "conversation.conversationEnd","conversation.divisionIds",  "conversation.originatingDirection",  "conversation.participants")

# COMMAND ----------

# DBTITLE 1,Dataset Enrichment & Schema Alignment
df_email = selected_df.withColumn("CONVERSATION_TYPE", F.lit("VOICE"))

if "resolutions" not in df_email.columns:
  print("Add missing resolutions column")
  df_email = df_email.withColumn("resolutions", F.array(F.struct(F.lit("").alias("eventTime"))))
  
df_email.createOrReplaceTempView("genesys_voice")

# COMMAND ----------

# DBTITLE 1,Create Normalized Conversations View (Flatten Parent-Level Fields)
# MAGIC %sql
# MAGIC create or replace temp view conversations_with_childs as
# MAGIC   select 
# MAGIC     g.conversationid as CONVERSATION_ID
# MAGIC     ,g.conversationStart as CONVERSATION_START
# MAGIC     ,g.conversationEnd as CONVERSATION_END
# MAGIC     --,resolutions
# MAGIC     ,case when g.resolutions is not null then g.resolutions[0].eventTime else null end as RESOLUTION_TIME
# MAGIC     --,element_at('struct(conversation.resolutions)', 1).eventTime as RESOLUTION_TIME
# MAGIC     --,regexp_extract(resolutions, "eventTime=([^,}]+)", 1) as RESOLUTION_TIME
# MAGIC     ,g.originatingDirection as ORIGINATING_DIRECTION
# MAGIC
# MAGIC     ,s.ani
# MAGIC     ,s.dnis
# MAGIC     
# MAGIC     , get_json_object(to_json(p.attributes), '$.LastIVRMenu') as last_ivr_menu
# MAGIC     , get_json_object(to_json(p.attributes), '$.IVR Menu Visited') as ivr_menu_visited
# MAGIC     ,sg.segmentStart as segmentstart
# MAGIC     
# MAGIC     -- Objects to be normalized
# MAGIC     ,g.participants as _PARTICIPANTS
# MAGIC     
# MAGIC     -- Excluded properties, tests added below
# MAGIC     ,g.divisionIds as _DIVISION_IDS
# MAGIC   from genesys_voice g
# MAGIC   
# MAGIC LATERAL VIEW OUTER explode(g.resolutions) r_view AS r
# MAGIC LATERAL VIEW OUTER explode(g.participants) p_view AS p
# MAGIC LATERAL VIEW OUTER explode(p.sessions) s_view AS s
# MAGIC LATERAL VIEW OUTER explode(s.segments) seg_view AS sg
# MAGIC

# COMMAND ----------

conversations_with_childs_df = spark.table("conversations_with_childs")

# Rename only the problematic keys
rename_map = {
    "IVR Menu Visited": "IVR_Menu_Visited",
    "Flow.TransferReason": "Flow_TransferReason",
    "ivr.LanguageSkill": "ivr_LanguageSkill",
    "ivr.Priority": "ivr_Priority",
    "ivr.Skills": "ivr_Skills",
}

# Get attribute field names from schema (driver-side)
attr_field_names = (
    conversations_with_childs_df.schema["_PARTICIPANTS"]
    .dataType.elementType["attributes"]
    .dataType.fieldNames()
)

conversations_with_childs_df = conversations_with_childs_df.withColumn(
    "_PARTICIPANTS",
    F.transform(
        F.col("_PARTICIPANTS"),
        lambda p: p.withField(
            "attributes",
            build_clean_attributes_struct(p.getField("attributes"))
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Explode Participants Array for Row-Level Normalization
# MAGIC %sql
# MAGIC create or replace temp view exploded_participants as (
# MAGIC   select
# MAGIC   distinct
# MAGIC   CONVERSATION_ID
# MAGIC   ,explode(_PARTICIPANTS) as PARTICIPANT
# MAGIC   from conversations_with_childs
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Ensure Optional Participant Attributes & Normalize Schema Structure
df_exploded_participants = spark.table("exploded_participants")

optional_columns = ["Q2_Name", "Q2Rating", "activity_id", "activity_type", "contact_id", "contact_type", "interaction_id"]
list_of_columns = list(df_exploded_participants.select("PARTICIPANT.attributes.*").columns)
struct_schema = []
struct_object = {}

for col in optional_columns:
  if col not in list_of_columns:
    struct_schema.append(
        StructField(col, StringType(), True)
    )
    struct_object[col] = F.lit(None)
    
df_exploded_participants = df_exploded_participants.withColumn("json_str", F.lit(str(struct_object)))\
        .withColumn("json_struct", F.from_json("json_str", StructType(struct_schema)))\
        .withColumn('ATTRIBUTES',
           F.struct(
            F.col('json_struct.*'),
            F.col('PARTICIPANT.attributes.*')
          )
        )\
        .drop("json_str","json_struct")


# COMMAND ----------

# DBTITLE 1,Define Participant & Attribute Field Mapping Keys for Flattening
participant_keys_list = ["participantId" ,"participantName" ,"purpose" ,"externalContactId" ,"teamId" ,"userId" ,"sessions"]
attributes_keys_list = ["ContactIDValue" ,"ConversationID" ,"CustomerSegment" ,"ExternalContactStatus" ,"IVRPayment","IVRPayment_Amount" ,"IVRPayment_OrderID" ,"IVRPayment_ReservationNo" ,"IVRPayment_ReservationType" ,"IVRProfile" ,"IVRType" ,"Language" ,"LastIVRMenu" , "IVR Menu Visited","NewConactID" ,"OrderStatus" ,"PaymentErrorCode" ,"PaymentErrorDescription" ,"Q1Rating" ,"Q1_Name" ,"Q2Rating" ,"Q2_Name" ,"SearchExternalContact" ,"SurveyDialed" ,"SurveyDialedTime" ,"TCApprove" ,"TCApproveLocal" ,"TransferReason" ,"TransferToSurvey" ,"WhatsAppDeflection" ,"activity_id" ,"activity_type" ,"agentname" ,"amount" ,"contact_id" ,"contact_type" ,"email" ,"firstname" ,"interaction_id" ,"`ivr.LanguageSkill`" ,"`ivr.Priority`" ,"`ivr.Skills`" ,"mobile" ,"orderid" ,"readyforpayment" ,"reservationType" ,"reservationno","PaymentStartTime","crm_mob"]

# COMMAND ----------

# DBTITLE 1,Dynamic Struct Flattening with Schema-Aware Column Handling
# ---------- working df ----------
df_exploded_participants_v2 = df_exploded_participants

df_exploded_participants_v2 = df_exploded_participants_v2.toDF(
    *[c.replace('`', '').replace('.', '_') for c in df_exploded_participants_v2.columns]
)

# ---------- schema-aware field lists ----------
participant_fields = (
    df_exploded_participants_v2.schema["PARTICIPANT"].dataType.names
    if "PARTICIPANT" in df_exploded_participants_v2.columns
    else []
)

attributes_fields = (
    df_exploded_participants_v2.schema["ATTRIBUTES"].dataType.names
    if "ATTRIBUTES" in df_exploded_participants_v2.columns
    else []
)

# ---------- missing trackers ----------
missing_participant_cols = []
missing_attributes_cols = []

# ---------- flatten PARTICIPANT ----------
for key in participant_keys_list:
    clean_key = normalize_key(key)
    out_col = clean_output_col(f"PARTICIPANT_{clean_key}")

    if clean_key in participant_fields:
        df_exploded_participants_v2 = df_exploded_participants_v2.withColumn(
            out_col,
            build_input_col("PARTICIPANT", key)
        )
    else:
        df_exploded_participants_v2 = df_exploded_participants_v2.withColumn(
            out_col,
            F.lit(None).cast("string")
        )
        missing_participant_cols.append(out_col)

# ---------- flatten ATTRIBUTES ----------
for key in attributes_keys_list:
    clean_key = normalize_key(key)
    out_col = clean_output_col(f"ATTRIBUTES_{clean_key}")

    if clean_key in attributes_fields:
        df_exploded_participants_v2 = df_exploded_participants_v2.withColumn(
            out_col,
            build_input_col("ATTRIBUTES", key)
        )
    else:
        df_exploded_participants_v2 = df_exploded_participants_v2.withColumn(
            out_col,
            F.lit(None).cast("string")
        )
        missing_attributes_cols.append(out_col)

# ---------- drop structs ----------
df_exploded_participants_v2 = df_exploded_participants_v2.drop("PARTICIPANT", "ATTRIBUTES")

# ---------- show result ----------
df_exploded_participants_v2.display()
df_exploded_participants_v2.createOrReplaceTempView("exploded_participants")

# ---------- print missing columns ----------
print("Missing PARTICIPANT columns (set to STRING NULL):")
for c in missing_participant_cols:
    print(f" - {c}")

print("\nMissing ATTRIBUTES columns (set to STRING NULL):")
for c in missing_attributes_cols:
    print(f" - {c}")


# COMMAND ----------

# DBTITLE 1,Create Participants View with Flattened Attributes & Business Transformations
# MAGIC %sql
# MAGIC   create or replace temp view participants_with_childs as 
# MAGIC    select
# MAGIC      CONVERSATION_ID
# MAGIC      ,PARTICIPANT_participantId as participant_id
# MAGIC      ,PARTICIPANT_participantName as NAME
# MAGIC      ,ATTRIBUTES_crm_mob as participant_mobile
# MAGIC      ,PARTICIPANT_purpose as PURPOSE
# MAGIC      ,PARTICIPANT_externalContactId as EXTERNAL_CONTACT_ID
# MAGIC      ,PARTICIPANT_teamId as TEAM_ID
# MAGIC      ,PARTICIPANT_userId as USER_ID
# MAGIC      ,ATTRIBUTES_ContactIDValue as ATTRIBUTE_CONTACT_ID_VALUE 
# MAGIC      ,ATTRIBUTES_ConversationID as ATTRIBUTE_CONVERSATION_ID 
# MAGIC      ,case when ATTRIBUTES_CustomerSegment = "NA" then null
# MAGIC        else ATTRIBUTES_CustomerSegment end as ATTRIBUTE_CUSTOMER_SEGMENT 
# MAGIC      ,ATTRIBUTES_ExternalContactStatus as ATTRIBUTE_EXTERNAL_CONTACT_STATUS
# MAGIC      ,ATTRIBUTES_IVRPayment as ATTRIBUTE_IVR_PAYMENT 
# MAGIC      ,ATTRIBUTES_IVRPayment_Amount as ATTRIBUTE_IVR_PAYMENT_AMOUNT 
# MAGIC      ,ATTRIBUTES_IVRPayment_OrderID as ATTRIBUTE_IVR_PAYMENT_ORDER_ID
# MAGIC      ,ATTRIBUTES_IVRPayment_ReservationNo as ATTRIBUTE_IVR_PAYMENT_RESERVATION_NO 
# MAGIC      ,ATTRIBUTES_IVRPayment_ReservationType as ATTRIBUTE_IVR_PAYMENT_RESERVATION_TYPE
# MAGIC      ,ATTRIBUTES_IVRProfile as ATTRIBUTE_IVR_PROFILE
# MAGIC      ,ATTRIBUTES_IVRType as ATTRIBUTE_IVR_TYPE
# MAGIC      ,ATTRIBUTES_Language as ATTRIBUTE_LANGUAGE
# MAGIC      ,ATTRIBUTES_LastIVRMenu as ATTRIBUTE_LAST_IVR_MENU 
# MAGIC      ,`ATTRIBUTES_IVR Menu Visited` as ATTRIBUTE_IVR_MENU_VISITED
# MAGIC      ,ATTRIBUTES_NewConactID as ATTRIBUTE_NEW_CONTACT_ID
# MAGIC      ,ATTRIBUTES_OrderStatus as ATTRIBUTE_ORDER_STATUS
# MAGIC      ,ATTRIBUTES_PaymentErrorCode as ATTRIBUTE_PAYMENT_ERROR_CODE 
# MAGIC      ,ATTRIBUTES_PaymentErrorDescription as ATTRIBUTE_ERROR_DESCRIPTION 
# MAGIC      ,to_timestamp(ATTRIBUTES_PaymentStartTime) as ATTRIBUTE_PAYMENT_START_TIME 
# MAGIC      ,ATTRIBUTES_Q1Rating as ATTRIBUTE_Q1_RATING 
# MAGIC      ,ATTRIBUTES_Q1_Name as ATTRIBUTE_Q1_NAME 
# MAGIC      ,ATTRIBUTES_Q2Rating as ATTRIBUTE_Q2_RATING 
# MAGIC      ,ATTRIBUTES_Q2_Name as ATTRIBUTE_Q2_NAME 
# MAGIC      ,ATTRIBUTES_SearchExternalContact as ATTRIBUTE_SEARCH_EXTERNAL_CONTACT
# MAGIC      ,case when ATTRIBUTES_SurveyDialed = "Yes" then true else false end as ATTRIBUTE_SURVEY_DIALED 
# MAGIC      ,ATTRIBUTES_SurveyDialedTime as ATTRIBUTE_SURVEY_DIALED_TIME 
# MAGIC      ,case when ATTRIBUTES_TCApprove = "NA" then null else ATTRIBUTES_TCApprove end as ATTRIBUTE_TC_APPROVE 
# MAGIC      ,ATTRIBUTES_TCApproveLocal as ATTRIBUTE_TC_APPROVE_LOCAL 
# MAGIC      ,ATTRIBUTES_TransferReason as ATTRIBUTE_TRANSFER_REASON 
# MAGIC      ,ATTRIBUTES_TransferToSurvey as ATTRIBUTE_TRANSFER_TO_SURVEY 
# MAGIC      ,ATTRIBUTES_WhatsAppDeflection as ATTRIBUTE_WHATSAPP_DEFLECTION  
# MAGIC      ,ATTRIBUTES_activity_id as ATTRIBUTE_ACTIVITY_ID 
# MAGIC      ,ATTRIBUTES_activity_type as ATTRIBUTE_ACTIVITY_TYPE 
# MAGIC      ,ATTRIBUTES_agentname as ATTRIBUTE_AGENT_NAME 
# MAGIC      ,ATTRIBUTES_amount as ATTRIBUTE_AMOUNT 
# MAGIC      ,ATTRIBUTES_contact_id as ATTRIBUTE_CONTACT_ID 
# MAGIC      ,ATTRIBUTES_contact_type as ATTRIBUTE_CONTACT_TYPE 
# MAGIC      ,lower(ATTRIBUTES_email) as ATTRIBUTE_EMAIL 
# MAGIC      ,ATTRIBUTES_firstname as ATTRIBUTE_FIRSTNAME 
# MAGIC      ,ATTRIBUTES_interaction_id as ATTRIBUTE_INTERACTION_ID 
# MAGIC      ,ATTRIBUTES_ivr_LanguageSkill as ATTRIBUTE_IVR_LANGUAGE_SKILLS 
# MAGIC      ,ATTRIBUTES_ivr_Priority as ATTRIBUTE_IVR_PRIORITY 
# MAGIC      ,ATTRIBUTES_ivr_Skills as ATTRIBUTE_IVR_SKILLS 
# MAGIC      ,ATTRIBUTES_mobile as ATTRIBUTE_MOBILE 
# MAGIC      ,ATTRIBUTES_orderid as ATTRIBUTE_ORDER_ID 
# MAGIC      ,ATTRIBUTES_readyforpayment as ATTRIBUTE_READY_FOR_PAYMENT 
# MAGIC      ,ATTRIBUTES_reservationType as ATTRIBUTE_RESERVATION_TYPE 
# MAGIC      ,ATTRIBUTES_reservationno as ATTRIBUTE_RESERVATION_NO 
# MAGIC      ,PARTICIPANT_sessions as _SESSIONS
# MAGIC
# MAGIC    from exploded_participants

# COMMAND ----------

# DBTITLE 1,Explode Participant Sessions for Session-Level Normalization
# MAGIC %sql
# MAGIC create or replace temp view sessions_exploded as
# MAGIC   select 
# MAGIC     CONVERSATION_ID
# MAGIC     ,participant_id as PARTICIPANT_ID
# MAGIC     ,explode(_SESSIONS) as SESSIONS
# MAGIC   from participants_with_childs 
# MAGIC   where _SESSIONS is not null

# COMMAND ----------

# DBTITLE 1,Validate Session Arrays Contain Single Values
df_sessions_exploded = spark.table("sessions_exploded")
assert df_sessions_exploded.select(F.size(F.col("SESSIONS.activeSkillIds")).alias("s")).filter(F.col("s") > 1).count() == 0
assert df_sessions_exploded.select(F.size(F.col("SESSIONS.destinationAddresses")).alias("s")).filter(F.col("s") > 1).count() == 0

# COMMAND ----------

# DBTITLE 1,Ensure Optional Flow Fields Exist & Standardize Session Schema
optional_columns = ["flowOutType"]
list_of_columns = list(df_sessions_exploded.select("SESSIONS.flow.*").columns)
struct_schema = []
struct_object = {}

for col in optional_columns:
  if col not in list_of_columns:
    struct_schema.append(
        StructField(col, StringType(), True)
    )
    struct_object[col] = F.lit(None)
    
df_sessions_exploded = df_sessions_exploded.withColumn("json_str", F.lit(str(struct_object)))\
        .withColumn("json_struct", F.from_json("json_str", StructType(struct_schema)))\
        .withColumn('SESSIONS_OPTIONAL',
           F.struct(
            F.col('json_struct.*'),
            F.col('SESSIONS.flow.*')
          )
        )\
        .drop("json_str","json_struct")

df_sessions_exploded.createOrReplaceTempView("sessions_exploded")

# COMMAND ----------

# DBTITLE 1,Create Sessions View with Flattened Flow, Routing & Metadata Fields
# MAGIC  %sql
# MAGIC  create or replace temp view sessions_with_childs as
# MAGIC    select
# MAGIC      CONVERSATION_ID
# MAGIC      ,PARTICIPANT_ID
# MAGIC      ,SESSIONS["sessionId"] as SESSION_ID
# MAGIC      ,SESSIONS["peerId"] as PEER_SESSION_ID
# MAGIC    
# MAGIC      ,SESSIONS["ani"] as ANI
# MAGIC      ,SESSIONS["dnis"] as DNIS
# MAGIC      ,SESSIONS["protocolCallId"] AS PROTOCOL_CALL_ID
# MAGIC      ,SESSIONS["recording"] as RECORDING
# MAGIC      ,SESSIONS["remoteNameDisplayable"] as REMOTE_NAME_DISPLAYABLE
# MAGIC      ,SESSIONS["activeSkillIds"][0] as ACTIVE_SKILL_ID 
# MAGIC      ,lower(SESSIONS["destinationAddresses"][0]) as DESTINATION_ADDRESS
# MAGIC      ,SESSIONS["mediaType"] as MEDIA_TYPE
# MAGIC      --,SESSIONS["agentBullseyeRing"] as AGENT_BULLSEYE_RING
# MAGIC      ,SESSIONS["direction"] as DIRECTION
# MAGIC      ,SESSIONS["flow"]["flowId"] as FLOW_ID
# MAGIC      ,SESSIONS["flow"]["flowVersion"] as FLOW_VERSION
# MAGIC      ,SESSIONS["flow"]["flowName"] as FLOW_NAME 
# MAGIC      ,SESSIONS["flow"]["flowType"] as FLOW_TYPE 
# MAGIC      ,SESSIONS["flow"]["entryReason"] as FLOW_ENTRY_REASON
# MAGIC      ,SESSIONS["flow"]["exitReason"] as FLOW_EXIT_REASON
# MAGIC      ,SESSIONS["flow"]["entryType"] as FLOW_ENTRY_TYPE
# MAGIC      ,SESSIONS["flow"]["transferTargetName"] as FLOW_TRANSFER_TARGET_NAME
# MAGIC      ,SESSIONS["flow"]["transferType"] as FLOW_TRANSFER_TYPE
# MAGIC      ,SESSIONS["flow"]["transferTargetAddress"] as FLOW_TRANSFER_TARGET_ADDRESS
# MAGIC      ,SESSIONS["flowInType"] as FLOW_IN_TYPE
# MAGIC      ,SESSIONS_OPTIONAL["flowOutType"] as FLOW_OUT_TYPE
# MAGIC    
# MAGIC      ,SESSIONS["remote"] as REMOTE
# MAGIC      ,SESSIONS["usedRouting"] as USED_ROUTING
# MAGIC      --,SESSIONS["routingRing"] as ROUTING_RING
# MAGIC      ,SESSIONS["selectedAgentId"] as SELECTED_AGENT_ID
# MAGIC      
# MAGIC      
# MAGIC      
# MAGIC      -- Objects to be normalized
# MAGIC      ,SESSIONS["segments"] as _SEGMENTS
# MAGIC      ,SESSIONS["metrics"] as _METRICS
# MAGIC      
# MAGIC      -- Excluded properties, tests added below
# MAGIC      ,SESSIONS["requestedRoutings"] as _REQUESTED_ROUTINGS
# MAGIC      ,SESSIONS["provider"] as _PROVIDER
# MAGIC      ,SESSIONS["edgeId"] as _EDGE_ID 
# MAGIC  
# MAGIC    from sessions_exploded

# COMMAND ----------

# DBTITLE 1,Session-Level Data Integrity & Consistency Validation Checks
df_sessions_with_childs = spark.table("sessions_with_childs")
# Checks if sessionId is unique
assert df_sessions_with_childs.count() == df_sessions_with_childs.dropDuplicates(["session_id"]).count()

# Checks if requestedRoutings has more than one element
assert df_sessions_with_childs.filter(F.size(F.col("_REQUESTED_ROUTINGS")) > 1).count() == 0
# Checks if the one element that requestedRoutings has is equal to usedRouting
# assert df_sessions_with_childs.filter(F.col("_REQUESTED_ROUTINGS")[0] != F.col("USED_ROUTING")).count() == 0

# COMMAND ----------

# DBTITLE 1,Aggregate Session Segments to Derive Session Start, End & Segment Count
# MAGIC %sql
# MAGIC create or replace temp view segment_aggregates as
# MAGIC   with segments as (
# MAGIC     select 
# MAGIC       SESSION_ID
# MAGIC       ,explode(_SEGMENTS) as SEGMENT
# MAGIC     from sessions_with_childs
# MAGIC   )
# MAGIC   select 
# MAGIC     SESSION_ID
# MAGIC     ,count(*) as NO_OF_SUB_SEGMENTS
# MAGIC     ,MAX(SEGMENT["segmentStart"]) as SESSION_START
# MAGIC     ,MIN(SEGMENT["segmentEnd"]) as SESSION_END
# MAGIC   from segments
# MAGIC   group by 1

# COMMAND ----------

# DBTITLE 1,Validate Segment Aggregation Integrity & Join Readiness
df_segment_details = spark.table("segment_aggregates")
# Checks if sessionId is unique as used for left-join
assert df_segment_details.count() == df_segment_details.dropDuplicates(["session_id"]).count()
# Checks if sessionId is the same size than sessions table
assert df_segment_details.count() == df_sessions_with_childs.count()

# COMMAND ----------

# DBTITLE 1,Create Sessions View with Aggregated Segment Metrics Added
# MAGIC %sql
# MAGIC create or replace temp view sessions_enriched_with_childs as 
# MAGIC   select 
# MAGIC     s.*
# MAGIC     ,sa.* except(sa.session_id)
# MAGIC   from sessions_with_childs s
# MAGIC   left join segment_aggregates sa on s.session_id = sa.session_id

# COMMAND ----------

# DBTITLE 1,Write conversation df
conversations_with_childs_df = spark.table("conversations_with_childs")

table_name = "voice_conversations"
target_table = f"ingestion_{env}.lnd_{datastore_name}.{dataset_name}_{table_name}"

# Deduplicate like client
# conversations_with_childs_df = make_df_unique(
#     conversations_with_childs_df,
#     unique_col="conversation_id",
#     order_by_col1="adf_load_time"
# )

window_spec = Window.partitionBy("conversation_id").orderBy(F.col("segmentstart").asc())
conversations_with_childs_df = (
    conversations_with_childs_df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)


df_conversations = prepare_writing_dataframe(conversations_with_childs_df)

write_increment(
    df=df_conversations,
    merge_clause="s.conversation_id = u.conversation_id",
    target_table=target_table
)

# COMMAND ----------

# DBTITLE 1,Write participants df
participants_with_childs_df = spark.table("participants_with_childs")

table_name = "voice_participants"
target_table = f"ingestion_{env}.lnd_{datastore_name}.{dataset_name}_{table_name}"

participants_with_childs_df = prepare_writing_dataframe(participants_with_childs_df)

window_spec = Window.partitionBy("participant_id", "conversation_id").orderBy(F.col("adf_load_time").desc())
participants_with_childs_df = (
    participants_with_childs_df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

assert participants_with_childs_df.count() == participants_with_childs_df.dropDuplicates(["participant_id"]).count()

write_increment(
    df=participants_with_childs_df,
    merge_clause="s.participant_id = u.participant_id and s.conversation_id = u.conversation_id",
    target_table=target_table
)

# COMMAND ----------

# DBTITLE 1,Write sessions df
df_sessions_enriched_with_childs = spark.table("sessions_enriched_with_childs")

table_name = "voice_sessions"
target_table = f"ingestion_{env}.lnd_{datastore_name}.{dataset_name}_{table_name}"

df_sessions_enriched_with_childs = prepare_writing_dataframe(df_sessions_enriched_with_childs)

window_spec = Window.partitionBy("session_id", "conversation_id").orderBy(F.col("adf_load_time").desc())
df_sessions_enriched_with_childs = (
    df_sessions_enriched_with_childs
    .withColumn("rn", F.row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

write_increment(
    df=df_sessions_enriched_with_childs,
    merge_clause="s.session_id = u.session_id and s.conversation_id = u.conversation_id",
    target_table=target_table
)

# COMMAND ----------

