# Databricks notebook source
# DBTITLE 1,Import Standard Libraries
import requests
import re
import base64
import json
import time
import pandas as pd
from pyspark.sql.functions import explode, col, lit,DataFrame,expr, from_json, schema_of_json
from pyspark.sql.types import StructType, StringType
from datetime import datetime,date,timedelta
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import pyspark.sql as SQL

# COMMAND ----------

# DBTITLE 1,Widgets & Parameters
dbutils.widgets.text('start_date', '')
dbutils.widgets.text('end_date', '')
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("datastore_name", "genesys")
dbutils.widgets.text("schema_name", "customer360")
dbutils.widgets.text("write_mode", "append")
dbutils.widgets.text("secret_name", "cloud-genesys-api-prd-apikey")

# COMMAND ----------

start_date = dbutils.widgets.get('start_date')
end_date = dbutils.widgets.get('end_date')
env = dbutils.widgets.get("env")
datastore_name = dbutils.widgets.get("datastore_name")
dataset_name = dbutils.widgets.get("schema_name")
write_mode = dbutils.widgets.get("write_mode")
secret_name = dbutils.widgets.get("secret_name")
root_directory = f"abfss://ingestion@stmxdatadatalake{env}01.dfs.core.windows.net"
adf_load_time = datetime.now(ZoneInfo("Asia/Dubai"))

# COMMAND ----------

from datetime import date, timedelta
from pyspark.sql import functions as F

# Create schema if not existed already
spark.sql(f"CREATE SCHEMA IF NOT EXISTS ingestion_{env}.lnd_{datastore_name} MANAGED LOCATION '{root_directory}/landing/lnd_{datastore_name}'")

table_name = "email_conversations"
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
            .select(F.max(F.to_date("segment_start")).alias("max_segment_start"))
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

# DBTITLE 1,Create function to write Incremental data
def prepare_writing_dataframe(df: SQL.DataFrame) -> SQL.DataFrame:
    df = df.toDF(*(c.lower() for c in df.columns))
    df = ( df.withColumn("adf_load_time", F.lit(adf_load_time.isoformat())) )
    return df.drop(*[c for c in df.columns if c.startswith("_")])

def write_increment(df: SQL.DataFrame, merge_clause: str, target_table: str) -> None:
    df = df.toDF(*(c.lower() for c in df.columns))
    print("Renamed Cols")
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
        print("Table does not exist")
        # print(df.head())
        (
            df.write
            .format("delta")
            .mode("error")
            .saveAsTable(target_table)
        )
        print(f"Created table {target_table}")

# COMMAND ----------

# DBTITLE 1,Encode API Credentials
client_id = '062ebeb7-a9e8-4c81-9fb3-16c381164fa8'
client_secret = dbutils.secrets.get(
    scope=f'kv-mx-data-adf-inges-{env}',
    key= secret_name
)
credentials = f"{client_id}:{client_secret}"
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
adf_load_time = datetime.now(ZoneInfo("Asia/Dubai"))

# COMMAND ----------

# DBTITLE 1,Token Generation
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

# DBTITLE 1,Create Conversation Job
url = "https://api.mypurecloud.de/api/v2/analytics/conversations/details/jobs"

# Parameters to be sent in the request body
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
                    "value": "email"
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
    'Content-Type': 'application/json'  # Ensure the content type is set to JSON
}

# Make a POST request with the parameters
response = requests.post(url, headers=headers, json=params)

# Print the response text
print(response.text)

data = response.json()
job_id = data["jobId"]


# COMMAND ----------

# DBTITLE 1,Check Job Completion

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

# DBTITLE 1,Extract Conversation Data
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

# DBTITLE 1,Exit Notebook if No Data
if str(count_of_records) == '0' :
  dbutils.notebook.exit("No incremental data available")

# COMMAND ----------

# DBTITLE 1,Get Required Fields

selected_df = exploded_df.select("conversation.conversationId","conversation.conversationStart", "conversation.conversationEnd","conversation.divisionIds",  "conversation.originatingDirection",  "conversation.resolutions",  "conversation.participants")

# COMMAND ----------

# DBTITLE 1,Add Missing Columns
df_email = selected_df.withColumn("CONVERSATION_TYPE", F.lit("EMAIL"))

if "resolutions" not in df_email.columns:
  print("Add missing resolutions column")
  df_email = df_email.withColumn("resolutions", F.array(F.struct(F.lit("").alias("eventTime"))))

if "conversationEnd" not in df_email.columns:
  print("Add missing conversationEnd column")
  df_email = df_email.withColumn("conversationEnd", F.lit(None))
  
df_email.createOrReplaceTempView("genesys_email")

# COMMAND ----------

# DBTITLE 1,Prepare Conversations Data
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW conversations_with_childs AS
# MAGIC SELECT
# MAGIC     g.conversationId                AS CONVERSATION_ID,
# MAGIC     g.conversationStart             AS CONVERSATION_START,
# MAGIC     g.conversationEnd               AS CONVERSATION_END,
# MAGIC     g.originatingDirection          AS ORIGINATING_DIRECTION,
# MAGIC     sg.subject                      AS EMAIL_SUBJECT,
# MAGIC     g.participants                  AS _PARTICIPANTS,
# MAGIC
# MAGIC     -- resolutions (array → struct)
# MAGIC     r.eventTime                     AS RESOLUTION_TIME,
# MAGIC
# MAGIC     -- flow / session fields 
# MAGIC     s.flow.entryType                AS entryType,
# MAGIC     s.addressFrom                   AS ADDRESS_FROM,
# MAGIC     s.addressTo                     AS ADDRESS_TO,
# MAGIC     s.addressOther                  AS ADDRESS_OTHER,
# MAGIC     s.addressSelf                   AS ADDRESS_SELF,
# MAGIC     s.direction                     AS DIRECTION,
# MAGIC
# MAGIC     -- session-level 
# MAGIC     s.flowInType                    AS FLOW_IN_TYPE,
# MAGIC     s.remote                        AS REMOTE,
# MAGIC     s.usedRouting                   AS USED_ROUTING,
# MAGIC
# MAGIC     -- flow struct
# MAGIC     s.flow.flowId                   AS FLOW_ID,
# MAGIC     s.flow.flowVersion              AS FLOW_VERSION,
# MAGIC     s.flow.flowName                 AS FLOW_NAME,
# MAGIC     s.flow.flowType                 AS FLOW_TYPE,
# MAGIC     s.flow.entryReason              AS FLOW_ENTRY_REASON,
# MAGIC     s.flow.exitReason               AS FLOW_EXIT_REASON,
# MAGIC     s.flow.entryType                AS FLOW_ENTRY_TYPE,
# MAGIC     s.flow.transferTargetName       AS FLOW_TRANSFER_TARGET_NAME,
# MAGIC     s.flow.transferType             AS FLOW_TRANSFER_TYPE,
# MAGIC     s.flow.transferTargetAddress    AS FLOW_TRANSFER_TARGET_ADDRESS,
# MAGIC
# MAGIC     s.sessionId                     AS SESSION_ID,
# MAGIC     sg.segmentStart                 as segment_start,
# MAGIC     sg.segmentEnd                   as segment_end
# MAGIC
# MAGIC FROM genesys_email g
# MAGIC LATERAL VIEW OUTER explode(g.resolutions) r_view AS r
# MAGIC LATERAL VIEW OUTER explode(g.participants) p_view AS p
# MAGIC LATERAL VIEW OUTER explode(p.sessions) s_view AS s
# MAGIC LATERAL VIEW OUTER explode(s.segments) seg_view AS sg
# MAGIC

# COMMAND ----------

expected_cols = [
    "AnnualPassHolder_HighValueCustomer",
    "EMAIL_FLOW",
    "Email_Preview_Transfer",
    "InquiryType",
    "CustomerEmailAddress",
    "DataTableLookup",
    "CustomerCommunicationID",
    "CustomerContactID",
    "ParkName"
]

df = spark.table("conversations_with_childs")

# get _PARTICIPANTS element schema
participants_schema = df.schema["_PARTICIPANTS"].dataType.elementType
attributes_schema = participants_schema["attributes"].dataType

existing_cols = [f.name for f in attributes_schema.fields]
missing_cols = [c for c in expected_cols if c not in existing_cols]

print("Existing columns in attributes:")
print(existing_cols)

print("\nMissing columns in attributes:")
print(missing_cols)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW participants_with_childs AS
# MAGIC SELECT
# MAGIC     c.CONVERSATION_ID,
# MAGIC
# MAGIC     p.participantId        AS participant_id,
# MAGIC     p.participantName      AS NAME,
# MAGIC     p.purpose              AS PURPOSE,
# MAGIC     p.externalContactId    AS EXTERNAL_CONTACT_ID,
# MAGIC     p.teamId               AS TEAM_ID,
# MAGIC     p.userId               AS USER_ID,
# MAGIC
# MAGIC     -- attributes
# MAGIC     get_json_object(to_json(p.attributes), '$.AnnualPassHolder_HighValueCustomer')
# MAGIC         AS ATTRIBUTE_ANNUAL_PASS_HOLDER_HIGH_VALUE_CUSTOMER,
# MAGIC
# MAGIC     get_json_object(to_json(p.attributes), '$.EMAIL_FLOW')
# MAGIC         AS ATTRIBUTE_EMAIL_FLOW,
# MAGIC
# MAGIC     get_json_object(to_json(p.attributes), '$.Email_Preview_Transfer')
# MAGIC         AS ATTRIBUTE_PREVIEW_TRANSFER,
# MAGIC
# MAGIC     get_json_object(to_json(p.attributes), '$.InquiryType')
# MAGIC         AS ATTRIBUTE_INQUIRY_TYPE,
# MAGIC
# MAGIC     coalesce( 
# MAGIC              lower(get_json_object(to_json(p.attributes), '$.senderEmailaddress'))
# MAGIC              ,lower(get_json_object(to_json(p.attributes), '$.CustomerEmailAddress'))
# MAGIC              ,lower(get_json_object(to_json(p.attributes), '$.SenderEmailAddress'))
# MAGIC     )
# MAGIC         AS ATTRIBUTE_CUSTOMER_EMAIL_ADDRESS,
# MAGIC
# MAGIC     lower(get_json_object(to_json(p.attributes), '$.DataTableLookup'))
# MAGIC         AS ATTRIBUTE_RESPONSE_EMAIL_ADDRESS,
# MAGIC
# MAGIC     get_json_object(to_json(p.attributes), '$.CustomerCommunicationID')
# MAGIC         AS ATTRIBUTE_CUSTOMER_COMMUNICATION_ID,
# MAGIC
# MAGIC     get_json_object(to_json(p.attributes), '$.CustomerContactID')
# MAGIC         AS ATTRIBUTE_CUSTOMER_CONTACT_ID,
# MAGIC
# MAGIC     get_json_object(to_json(p.attributes), '$.ParkName')
# MAGIC         AS ATTRIBUTE_PARK_NAME,
# MAGIC
# MAGIC     -- object to normalize later
# MAGIC     p.sessions AS _SESSIONS
# MAGIC
# MAGIC FROM conversations_with_childs c
# MAGIC LATERAL VIEW OUTER explode(c._PARTICIPANTS) p_view AS p;

# COMMAND ----------

# DBTITLE 1,Join Sessions with Aggregates
# MAGIC %sql
# MAGIC create or replace temp view sessions_enriched_with_childs as 
# MAGIC   with segment_aggregates as (
# MAGIC     SELECT
# MAGIC     SESSION_ID,
# MAGIC     COUNT(distinct segment_start ) AS NO_OF_SUB_SEGMENTS,
# MAGIC     MIN(segment_start)   AS SESSION_START,
# MAGIC     MAX(segment_end)     AS SESSION_END
# MAGIC FROM conversations_with_childs
# MAGIC GROUP BY SESSION_ID
# MAGIC   )
# MAGIC
# MAGIC select 
# MAGIC     s.CONVERSATION_ID,
# MAGIC
# MAGIC     s.SESSION_ID as SESSION_ID,
# MAGIC     sa.* except(sa.SESSION_ID),
# MAGIC
# MAGIC     s.entryType,
# MAGIC     s.ADDRESS_FROM,
# MAGIC     s.ADDRESS_OTHER,
# MAGIC     s.ADDRESS_SELF,
# MAGIC     s.ADDRESS_TO,
# MAGIC     s.DIRECTION,
# MAGIC
# MAGIC     s.EMAIL_SUBJECT,
# MAGIC     
# MAGIC     s.FLOW_IN_TYPE,
# MAGIC     s.REMOTE,
# MAGIC     s.USED_ROUTING,
# MAGIC
# MAGIC     
# MAGIC     s.FLOW_ID,
# MAGIC     s.FLOW_VERSION,
# MAGIC     s.FLOW_NAME,
# MAGIC     s.FLOW_TYPE,
# MAGIC     s.FLOW_ENTRY_REASON,
# MAGIC     s.FLOW_EXIT_REASON,
# MAGIC     s.FLOW_ENTRY_TYPE,
# MAGIC     s.FLOW_TRANSFER_TARGET_NAME,
# MAGIC     s.FLOW_TRANSFER_TYPE,
# MAGIC     s.FLOW_TRANSFER_TARGET_ADDRESS,
# MAGIC
# MAGIC
# MAGIC     s.segment_start,
# MAGIC     s.segment_end
# MAGIC
# MAGIC   from conversations_with_childs s
# MAGIC   left join segment_aggregates sa on s.SESSION_ID = sa.SESSION_ID

# COMMAND ----------

# DBTITLE 1,Write conversations df
conversations_with_childs_df = spark.table("conversations_with_childs")

table_name = "email_conversations"
target_table = f"ingestion_{env}.lnd_{datastore_name}.{dataset_name}_{table_name}"

window_spec = Window.partitionBy("conversation_id").orderBy(
    F.col("FLOW_ID").desc_nulls_last(),
    F.col("segment_start").asc()
)
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

table_name = "email_participants"
target_table = f"ingestion_{env}.lnd_{datastore_name}.{dataset_name}_{table_name}"

df_participants = prepare_writing_dataframe(participants_with_childs_df)
window_spec = Window.partitionBy("participant_id", "conversation_id").orderBy(F.col("adf_load_time").desc())
df_participants = (
    df_participants
    .withColumn("rn", F.row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

assert df_participants.count() == df_participants.dropDuplicates(["participant_id"]).count()

write_increment(
    df=df_participants,
    merge_clause="s.participant_id = u.participant_id and s.conversation_id = u.conversation_id",
    target_table=target_table
)

# COMMAND ----------

# DBTITLE 1,Write sessions df
df_sessions_enriched_with_childs = spark.table("sessions_enriched_with_childs")

table_name = "email_sessions"
target_table = f"ingestion_{env}.lnd_{datastore_name}.{dataset_name}_{table_name}"

window_spec = Window.partitionBy("SESSION_ID", "conversation_id").orderBy(F.col("segment_start").asc())
df_sessions = (
    df_sessions_enriched_with_childs
    .withColumn("rn", F.row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)
df_sessions = prepare_writing_dataframe(df_sessions)

assert df_sessions.count() == df_sessions.dropDuplicates(["session_id"]).count()

write_increment(
    df=df_sessions,
    merge_clause="s.session_id = u.session_id and s.conversation_id = u.conversation_id",
    target_table=target_table
)