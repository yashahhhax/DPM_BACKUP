# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### This notebook used for platinum touchpoints table loading from genesys source data of email and voice. It consolidates both channels data in single table. Source Tables used for this notebook mentioned below.
# MAGIC
# MAGIC
# MAGIC ##  Table Summary
# MAGIC
# MAGIC |Source Tables|Use|
# MAGIC |------|-----|
# MAGIC |brz_genesys.vw_customer360_email_conversations|conversation and related email information|
# MAGIC |brz_genesys.vw_customer360_email_participants|participant_attributes aggregation|
# MAGIC |brz_genesys.vw_customer360_email_sessions|sessions and flow details|
# MAGIC |brz_genesys.vw_customer360_voice_conversations|conversation and related mobile contact information|
# MAGIC |brz_genesys.vw_customer360_voice_participants|participant_attributes aggregation|
# MAGIC |brz_genesys.vw_customer360_voice_sessions|sessions and flow details|
# MAGIC |plt_customer360.scv_account_master|scv_sk identification|
# MAGIC
# MAGIC |Target Table|
# MAGIC |---|
# MAGIC |plt_customer360.touchpoints|
# MAGIC
# MAGIC ### Schema enforcement implemented
# MAGIC
# MAGIC ### **Load Strategy:** Incremental (upsert based on conversation_id, by default load )

# COMMAND ----------

# DBTITLE 1,schema_config_common
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,base_functions
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %run ../../config/view_queries

# COMMAND ----------

# DBTITLE 1,get notebook params
try:
    notebook_vars = [
        'env','start_date','end_date','current_job_run_id','buffer_days'
    ]
    env,start_date,end_date,current_job_run_id,buffer_days = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,import dependencies
from pyspark.sql.functions import col, when, current_timestamp, trim, lower, lit, regexp_replace,regexp_extract, instr, split, row_number, to_date, date_format, date_sub, upper,min,max,expr,coalesce,length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime
from zoneinfo import ZoneInfo

# COMMAND ----------

# DBTITLE 1,schema/table_name
#source table bronze schema

bronze_schema = f"transformation_{env}.brz_genesys"

#target table schema

platinum_schema = f"transformation_{env}.plt_customer360"

#source table

email_conversations_table = f"{bronze_schema}.vw_customer360_email_conversations"
email_participants_table = f"{bronze_schema}.vw_customer360_email_participants"
email_sessions_table = f"{bronze_schema}.vw_customer360_email_sessions"

voice_conversations_table = f"{bronze_schema}.vw_customer360_voice_conversations"
voice_participants_table = f"{bronze_schema}.vw_customer360_voice_participants"
voice_sessions_table = f"{bronze_schema}.vw_customer360_voice_sessions"

scv_master_table = f"{platinum_schema}.scv_account_master"

#target table
target_table = f"{platinum_schema}.touchpoints"

print(f"email_conversations_table:{email_conversations_table}")
print(f"email_participants_table:{email_participants_table}")
print(f"email_sessions_table:{email_sessions_table}")
print(f"voice_conversations_table:{voice_conversations_table}")
print(f"voice_participants_table:{voice_participants_table}")
print(f"voice_sessions_table:{voice_sessions_table}")
print(f"scv_master_table:{scv_master_table}")
print(f"target_table:{target_table}")

# COMMAND ----------

# DBTITLE 1,Filter Condition
#--------------------determine load mode FIRST, then build filter condition--------------------
# Step 1 — determine watermark and load mode
if spark.catalog.tableExists(target_table):
    watermark = spark.sql(f"SELECT MAX(etl_updated_datetime) FROM {target_table}").collect()[0][0]
else:
    watermark = None

use_date_range = bool(start_date.strip() and end_date.strip())
is_full_load   = watermark is None and not use_date_range

# Step 2 — build filter_condition based on load mode
if use_date_range:
    if start_date < end_date:
        filter_condition = f"and adf_load_time between '{start_date}' and '{end_date}'"
    else:
        raise Exception("start_date must be less than end_date")
elif is_full_load:
    filter_condition = ''
else:
    max_date = spark.sql(f"SELECT date(MAX(etl_updated_datetime)) FROM {target_table}").collect()[0][0]
    filter_condition = f"and adf_load_time > ('{max_date}' - INTERVAL {buffer_days} days)" #loaded_at

# print(f"Load mode         : {'DATE RANGE' if use_date_range else 'FULL LOAD' if is_full_load else 'INCREMENTAL'}")
# print(f"Watermark         : {watermark}")
print(f"Filter condition  : {filter_condition}")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Email Touchpints

# COMMAND ----------

# DBTITLE 1,emai_src_incr
df_email_conversations = spark.sql(f"""
    select
    CONVERSATION_ID,
    CONVERSATION_START,
    CONVERSATION_END,
    RESOLUTION_TIME,
    ORIGINATING_DIRECTION,
    address_from,
    address_to,
    email_subject
    from {email_conversations_table} ec
    where 1=1
    {filter_condition}
    """).createOrReplaceTempView("email_conversations")

df_email_participants = spark.sql(f"""
                                  select ep.* from {email_participants_table} ep inner join email_conversations ec on ep.CONVERSATION_ID = ec.CONVERSATION_ID
                                  """).createOrReplaceTempView("email_participants")

df_email_sessions = spark.sql(f"""
                                  select es.* from {email_sessions_table} es inner join email_conversations ec on es.CONVERSATION_ID = ec.CONVERSATION_ID
                                  """).createOrReplaceTempView("email_sessions")

# COMMAND ----------

# DBTITLE 1,email_customer_participants
spark.sql(f"""
with email_base as (
select conversation_id, lower(trim(address_from)) as email from email_sessions
UNION
select conversation_id, lower(trim(address_to)) as email from email_sessions
UNION
select conversation_id, lower(trim(address_other)) as email from email_sessions
UNION
select conversation_id, lower(trim(address_self)) as email from email_sessions
)
select 
conversation_id 
,listagg(email,';') as participant_emails
from email_base 
where email is not null
and length(email) < 50
group by conversation_id
""").createOrReplaceTempView("vw_email_conversation_participant_emails")

# COMMAND ----------

# DBTITLE 1,email_participants_transofrmation
spark.sql(f"""
    select
    CONVERSATION_ID
    ,first(ATTRIBUTE_EMAIL_FLOW) filter(where ATTRIBUTE_EMAIL_FLOW is not null) as E2E_EMAIL_FLOW
    ,first(ATTRIBUTE_INQUIRY_TYPE) filter(where ATTRIBUTE_INQUIRY_TYPE is not null) as INQUIRY_TYPE
    --need check not null?
    ,first(ATTRIBUTE_RESPONSE_EMAIL_ADDRESS) filter(where ATTRIBUTE_RESPONSE_EMAIL_ADDRESS is not null) as ATTRIBUTE_RESPONSE_EMAIL_ADDRESS
    ,first(ATTRIBUTE_PARK_NAME) filter(where ATTRIBUTE_PARK_NAME is not null) as AFFECTED_ASSET
    ,sum(case when PURPOSE = "acd" then 1 else 0 end) as ACD_QUEUES_INVOLVED
    ,sum(case when PURPOSE = "agent" then 1 else 0 end) as AGENTS_INVOLVED
    ,sum(case when PURPOSE = "workflow" then 1 else 0 end) as WORKFLOWS_INVOLVED
    ,count(distinct participant_id) as TOTAL_CONVERSATION_PARTICIPANTS
    from email_participants
    group by 1
          """).createOrReplaceTempView("vw_email_participants")

# COMMAND ----------

# DBTITLE 1,email_sessions
spark.sql(f"""
    WITH email_sessions_ranked_flows AS (
    SELECT 
        es.CONVERSATION_ID,
        es.SESSION_START,
        es.SESSION_END,
        es.FLOW_ENTRY_REASON,
        es.FLOW_ENTRY_TYPE,
        es.FLOW_EXIT_REASON,
        es.FLOW_ID,
        es.FLOW_NAME,
        es.FLOW_TRANSFER_TARGET_NAME,
        es.FLOW_TRANSFER_TYPE,
        es.FLOW_TYPE,
        es.FLOW_VERSION,
        es.DIRECTION,
        RANK() OVER (
            PARTITION BY CONVERSATION_ID 
            ORDER BY SESSION_START DESC, SESSION_END DESC
        ) AS FLOW_RANK_DESC
    FROM email_sessions es
    WHERE FLOW_ID IS NOT NULL
    ),
    number_of_flows as(
        select conversation_id,
        MAX(esr.FLOW_RANK_DESC) AS NO_OF_INTERMEDIATE_FLOWS,
        SUM(CASE WHEN DIRECTION = 'inbound' THEN 1 ELSE 0 END) AS INBOUND_COMMUNICATION_COUNT,
        SUM(CASE WHEN DIRECTION = 'outbound' THEN 1 ELSE 0 END) AS OUTBOUND_COMMUNICATION_COUNT
        from email_sessions_ranked_flows esr
        group by 1
    )
    SELECT 
    esr.CONVERSATION_ID,
    -- Total number of flows/sessions per conversation
    NO_OF_INTERMEDIATE_FLOWS,
    esr.FLOW_ENTRY_REASON,
    esr.FLOW_ENTRY_TYPE,
    esr.FLOW_EXIT_REASON,
    esr.FLOW_ID,
    esr.FLOW_NAME,
    esr.FLOW_TRANSFER_TARGET_NAME,
    esr.FLOW_TRANSFER_TYPE,
    esr.FLOW_TYPE,
    esr.FLOW_VERSION,
    --base.* EXCEPT (DIRECTION),
    -- need to check if communication count will be lost then left join with only above columns cte
    INBOUND_COMMUNICATION_COUNT,
    OUTBOUND_COMMUNICATION_COUNT,
    FLOW_RANK_DESC
    FROM email_sessions_ranked_flows esr
    left join number_of_flows nf on esr.CONVERSATION_ID = nf.conversation_id
    WHERE FLOW_RANK_DESC = 1""").createOrReplaceTempView("vw_email_sessions")


# COMMAND ----------

# DBTITLE 1,scv_master_ranked_email
spark.sql(f"""
        select email_id, scv_account_sk from {scv_master_table}
        where source_name = 't_mdm_guest' --/// Added this to pick only parent
        qualify(ROW_NUMBER() OVER (PARTITION BY email_id ORDER BY  modified_date desc)) = 1
  
        """).createOrReplaceTempView("vw_scv_ranked_email")

# COMMAND ----------

# DBTITLE 1,Email_touchpoints_final
# current timestamp
current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

df_email_touchpoints =  spark.sql(f"""
        select 
        map('scv_sk',cast(scv.scv_account_sk as string),
          'omni_sk',NULL,
          'email_id', cast(ec.address_from as string)
          ) as entity_identifiers,
        scv.scv_account_sk as owner_scv_sk,
        NULL as owner_omni_sk,
        map(
        'email', cast(ec.address_from as string)
            ) as contact_profile,
        'EMAIL' as type,
        ec.CONVERSATION_ID as conversation_id,
        ec.CONVERSATION_START as conversation_start,
        ec.CONVERSATION_END as conversation_end,
        ec.RESOLUTION_TIME as resolution_time,
        ec.ORIGINATING_DIRECTION as originating_direction,
      -- Email participant attributes
        ep.INQUIRY_TYPE AS inquiry_type,
        ep.AFFECTED_ASSET AS affected_asset,
      -- Voice-only fields → NULL for email   
        CAST(NULL AS MAP<STRING, STRING>) AS ivr_details,
        CAST(NULL AS MAP<STRING, STRING>) AS payment_details,
      -- flow fields
         map(
            'e2e_email_flow', CAST(ep.E2E_EMAIL_FLOW AS STRING),
            'no_of_intermediate_flows', CAST(es.NO_OF_INTERMEDIATE_FLOWS AS STRING),
            'flow_entry_reason', CAST(es.FLOW_ENTRY_REASON AS STRING),
            'flow_entry_type', CAST(es.FLOW_ENTRY_TYPE AS STRING),
            'flow_exit_reason', CAST(es.FLOW_EXIT_REASON AS STRING),
            'flow_id', CAST(es.FLOW_ID AS STRING),
            'flow_name', CAST(es.FLOW_NAME AS STRING),
            'flow_rank_desc', CAST(es.FLOW_RANK_DESC AS STRING),
            'flow_transfer_target_name', CAST(es.FLOW_TRANSFER_TARGET_NAME AS STRING),
            'flow_transfer_type', CAST(es.FLOW_TRANSFER_TYPE AS STRING),
            'flow_type', CAST(es.FLOW_TYPE AS STRING),
            'flow_version', CAST(es.FLOW_VERSION AS STRING)
        ) as flow_details,

        es.INBOUND_COMMUNICATION_COUNT AS inbound_communication_count,
        es.OUTBOUND_COMMUNICATION_COUNT AS outbound_communication_count,
        -- Participant counts
        ep.TOTAL_CONVERSATION_PARTICIPANTS AS total_conversation_participants,
        ep.ACD_QUEUES_INVOLVED AS acd_queues_involved,
        ep.AGENTS_INVOLVED AS agents_involved,
        ep.WORKFLOWS_INVOLVED AS workflows_involved,
    -- Voice-only participant → NULL for email
      NULL AS ivr_involved,
      NULL AS voicemails_involved,
        map( 
        'response_mail_address', CAST(ep.ATTRIBUTE_RESPONSE_EMAIL_ADDRESS AS STRING)
        ,'participant_emails', CAST(cep.participant_emails AS STRING)
        ,'from_Address', CAST(ec.address_from AS STRING)
        ,'to_Address', CAST(ec.address_to AS STRING)
        ,'email_subject', CAST(ec.email_subject AS STRING)
        ) as others 
        ,'GENESYS_EMAIL_API' as source_name
        ,'{current_uae_timestamp}' as etl_created_datetime     
        ,'{current_uae_timestamp}' as etl_updated_datetime     
        from email_conversations ec
  -- below table contains address from detail but no column used in selection
        left join vw_email_conversation_participant_emails cep on ec.CONVERSATION_ID = cep.CONVERSATION_ID
        left join vw_email_participants ep on ec.CONVERSATION_ID = ep.CONVERSATION_ID
        left join vw_email_sessions es on ec.CONVERSATION_ID = es.CONVERSATION_ID
        left join vw_scv_ranked_email scv on {email_standardization('ec.address_from')} = scv.email_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Voice Touchpoints

# COMMAND ----------

# DBTITLE 1,voice_src_incr
df_voice_conversations = spark.sql(f"""
    select
    CONVERSATION_ID as CONVERSATION_ID,
    CONVERSATION_START as CONVERSATION_START,
    CONVERSATION_END as CONVERSATION_END,
    RESOLUTION_TIME as RESOLUTION_TIME,
    ivr_menu_visited,
    regexp_replace(ec.ani, '[^0-9]', '') as ani,
    regexp_replace(ec.dnis, '[^0-9]', '') as dnis,
    ORIGINATING_DIRECTION
    from {voice_conversations_table} ec
    where 1=1
    {filter_condition}
    """).createOrReplaceTempView("voice_conversations")

df_voice_participants = spark.sql(f"""
                                  select vp.* from {voice_participants_table} vp inner join voice_conversations vc on vp.CONVERSATION_ID = vc.CONVERSATION_ID
                                  """).createOrReplaceTempView("voice_participants")

df_voice_sessions = spark.sql(f"""
                                  select vs.* from {voice_sessions_table} vs inner join voice_conversations vc on vs.CONVERSATION_ID = vc.CONVERSATION_ID
                                  """).createOrReplaceTempView("voice_sessions")

# COMMAND ----------

# DBTITLE 1,voice_participant
spark.sql(f"""
          select
    CONVERSATION_ID
    ,first(ATTRIBUTE_IVR_PAYMENT) filter(where ATTRIBUTE_IVR_PAYMENT is not null) as ATTRIBUTE_IVR_PAYMENT -- 22754 null, 373 success, 60 failed
    ,first(ATTRIBUTE_IVR_PAYMENT_AMOUNT) filter(where ATTRIBUTE_IVR_PAYMENT_AMOUNT is not null) as ATTRIBUTE_IVR_PAYMENT_AMOUNT -- 22731 null 
    ,first(ATTRIBUTE_IVR_PAYMENT_ORDER_ID) filter(where ATTRIBUTE_IVR_PAYMENT_ORDER_ID is not null) as ATTRIBUTE_IVR_PAYMENT_ORDER_ID
    ,first(ATTRIBUTE_IVR_PAYMENT_RESERVATION_NO) filter(where ATTRIBUTE_IVR_PAYMENT_RESERVATION_NO is not null) as ATTRIBUTE_IVR_PAYMENT_RESERVATION_NO
    ,first(ATTRIBUTE_IVR_PAYMENT_RESERVATION_TYPE) filter(where ATTRIBUTE_IVR_PAYMENT_RESERVATION_TYPE is not null) as ATTRIBUTE_IVR_PAYMENT_RESERVATION_TYPE 
    ,first(ATTRIBUTE_IVR_PROFILE) filter(where ATTRIBUTE_IVR_PROFILE is not null) as ATTRIBUTE_IVR_PROFILE -- to others
    ,first(ATTRIBUTE_IVR_TYPE) filter(where ATTRIBUTE_IVR_TYPE is not null) as ATTRIBUTE_IVR_TYPE
    ,first(ATTRIBUTE_LAST_IVR_MENU) filter(where ATTRIBUTE_LAST_IVR_MENU is not null) as ATTRIBUTE_LAST_IVR_MENU
    ,first(ATTRIBUTE_PAYMENT_ERROR_CODE) filter(where ATTRIBUTE_PAYMENT_ERROR_CODE is not null) as ATTRIBUTE_PAYMENT_ERROR_CODE -- to others
    ,first(ATTRIBUTE_ERROR_DESCRIPTION) filter(where ATTRIBUTE_ERROR_DESCRIPTION is not null) as ATTRIBUTE_ERROR_DESCRIPTION
    ,first(ATTRIBUTE_PAYMENT_START_TIME) filter(where ATTRIBUTE_PAYMENT_START_TIME is not null) as ATTRIBUTE_PAYMENT_START_TIME
    ,sum(case when PURPOSE = "acd" then 1 else 0 end) as ACD_QUEUES_INVOLVED
    ,sum(case when PURPOSE = "agent" then 1 else 0 end) as AGENTS_INVOLVED
    ,sum(case when PURPOSE = "voicemail" then 1 else 0 end) as VOICEMAILS_INVOLVED
    ,sum(case when PURPOSE = "ivr" then 1 else 0 end) as IVR_INVOLVED
    ,count(PURPOSE) as TOTAL_CONVERSATION_PARTICIPANTS
  from voice_participants
  group by 1
          """).createOrReplaceTempView("vw_conversation_voice_participant_information")
  

# COMMAND ----------

# DBTITLE 1,voice_sessions
spark.sql(f"""
          with ranked_flows as (
    select 
      row_number() over(partition by CONVERSATION_ID order by SESSION_START desc, SESSION_END desc) as FLOW_RANK_DESC
      ,CONVERSATION_ID
      ,DIRECTION
      ,FLOW_ID
      ,FLOW_VERSION
      ,FLOW_NAME
      ,FLOW_TYPE
      ,FLOW_ENTRY_REASON
      ,FLOW_EXIT_REASON
      ,FLOW_ENTRY_TYPE
      ,FLOW_TRANSFER_TARGET_NAME
      ,FLOW_TRANSFER_TYPE
      ,SESSION_START
      ,SESSION_END
    from voice_sessions
    where FLOW_ID is not null
  )
  ,number_of_flows as (
    select 
      CONVERSATION_ID
      ,max(FLOW_RANK_DESC) as NO_OF_INTERMEDIATE_FLOWS
      ,SUM(case when DIRECTION = "inbound" then 1 else 0 end) as INBOUND_COMMUNICATION_COUNT
      ,SUM(case when DIRECTION = "outbound" then 1 else 0 end) as OUTBOUND_COMMUNICATION_COUNT
    from ranked_flows 
    group by 1
  )
  select 
    rf.* except(rf.DIRECTION, rf.SESSION_START, rf.SESSION_END)
    ,nof.NO_OF_INTERMEDIATE_FLOWS
    ,nof.INBOUND_COMMUNICATION_COUNT
    ,nof.OUTBOUND_COMMUNICATION_COUNT
  from ranked_flows rf
  left join number_of_flows nof on rf.CONVERSATION_ID = nof.CONVERSATION_ID
  where rf.FLOW_RANK_DESC = 1
          """).createOrReplaceTempView("vw_intermediate_conversation_voice_flows")  

# COMMAND ----------

# DBTITLE 1,ranked_scv_master_voice
spark.sql(f"""
        select concat(mobile_country_code, mobile_num) as mobile_num , scv_account_sk  from {scv_master_table}
        where source_name = 't_mdm_guest' --/// Added this to pick only parent
        qualify(row_number() over(partition by concat(mobile_country_code, mobile_num) order by modified_date desc,mdm_id)) = 1
        
        """).createOrReplaceTempView("vw_scv_ranked_voice")


# COMMAND ----------

# DBTITLE 1,voice_touchpoint_final
# current timestamp
current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

df_voice_touchpoints = spark.sql(f"""
    select
    map('scv_sk',cast(scv.scv_account_sk as string),
      'omni_sk',NULL,
      'email_id', NULL
      ) as entity_identifiers
    ,scv.scv_account_sk as owner_scv_sk
    ,NULL as owner_omni_sk
    ,map('ANI', cast(cv.ani as string) ) as contact_profile
    ,'VOICE' as type
    ,cv.CONVERSATION_ID
    ,cv.CONVERSATION_START
    ,cv.CONVERSATION_END
    ,cv.RESOLUTION_TIME
    ,cv.ORIGINATING_DIRECTION
    ,null as inquiry_type
    ,null as affected_asset
    ,map(
    'last_ivr_menu', cast(cvpi.ATTRIBUTE_LAST_IVR_MENU as string),
    'ivr_menu_visited', cast(cv.ivr_menu_visited as string),
    'ivr_type', cast(cvpi.ATTRIBUTE_IVR_TYPE as string),
    'ivr_profile', cast(cvpi.ATTRIBUTE_IVR_PROFILE as string)
    ) as ivr_details
    ,map(
    'ivr_payment_reservation_type', cast(cvpi.ATTRIBUTE_IVR_PAYMENT_RESERVATION_TYPE as string),
    'ivr_payment_reservation_no', cast(cvpi.ATTRIBUTE_IVR_PAYMENT_RESERVATION_NO as string),
    'ivr_payment_order_id', cast(cvpi.ATTRIBUTE_IVR_PAYMENT_ORDER_ID as string),
    'payment_status', cast(cvpi.ATTRIBUTE_IVR_PAYMENT as string),
    'payment_amount', cast(cvpi.ATTRIBUTE_IVR_PAYMENT_AMOUNT as string),
    'payment_start_time', cast(cvpi.ATTRIBUTE_PAYMENT_START_TIME as string),
    'resolution_error_description', cast(cvpi.ATTRIBUTE_ERROR_DESCRIPTION as string),
    'payment_error_code', cast(cvpi.ATTRIBUTE_PAYMENT_ERROR_CODE as string)
    ) as payment_details
    ,map(
    'no_of_intermediate_flows', cast(icvf.NO_OF_INTERMEDIATE_FLOWS as string),
    'flow_entry_reason', cast(icvf.FLOW_ENTRY_REASON as string),
    'flow_entry_type', cast(icvf.FLOW_ENTRY_TYPE as string),
    'flow_exit_reason', cast(icvf.FLOW_EXIT_REASON as string),
    'flow_id', cast(icvf.FLOW_ID as string),
    'flow_name', cast(icvf.FLOW_NAME as string),
    'flow_rank_desc', cast(icvf.FLOW_RANK_DESC as string),
    'flow_transfer_target_name', cast(icvf.FLOW_TRANSFER_TARGET_NAME as string),
    'flow_transfer_type', cast(icvf.FLOW_TRANSFER_TYPE as string),
    'flow_type', cast(icvf.FLOW_TYPE as string),
    'flow_version', cast(icvf.FLOW_VERSION as string)
    ) as flow_details
    
    ,icvf.INBOUND_COMMUNICATION_COUNT
    ,icvf.OUTBOUND_COMMUNICATION_COUNT
    
    ,cvpi.TOTAL_CONVERSATION_PARTICIPANTS
    ,cvpi.ACD_QUEUES_INVOLVED
    ,cvpi.AGENTS_INVOLVED
    ,null as workflows_involved
    ,IVR_INVOLVED
    ,VOICEMAILS_INVOLVED
    ,map(
      'ani',cv.ANI
      ,'dnis',cv.DNIS
    ) as others
    ,'GENESYS_VOICE_API' as source_name
    ,'{current_uae_timestamp}' as etl_created_datetime     
    ,'{current_uae_timestamp}' as etl_updated_datetime     
  from voice_conversations cv
  left join vw_conversation_voice_participant_information cvpi on cv.CONVERSATION_ID = cvpi.CONVERSATION_ID
  left join vw_intermediate_conversation_voice_flows icvf on cv.CONVERSATION_ID = icvf.CONVERSATION_ID                         
  --left join with scv for mobile
  left join vw_scv_ranked_voice scv on cv.ANI = scv.mobile_num
  """)

# COMMAND ----------

# DBTITLE 1,final_df_union
df_touchpoints = df_email_touchpoints.unionByName(df_voice_touchpoints)

# COMMAND ----------

# DBTITLE 1,schema casting
df_touchpoints = apply_schema_casting(df_touchpoints,touchpoints_schema)

# COMMAND ----------

# DBTITLE 1,writing to UC
# Build DDL string from schema dict

touchpoints_ddl = ",\n".join(
    f"`{col}` {dtype}" for col, dtype in touchpoints_schema.items()
)

# merge on conv_id --upsert

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(touchpoints_ddl)
    if compare_schema_lenient(df_touchpoints.schema, ddl_struct):
        print("Schema matches!")

        if use_date_range:
            print(f"Load mode : DATE RANGE ({start_date} to {end_date})")
        elif is_full_load:
            print(f"Load mode : FULL LOAD")
        else:
            print(f"Load mode : INCREMENTAL (watermark: {watermark})")

        if is_full_load:
            (df_touchpoints.write.format("delta")
                                 .mode("overwrite")
                                 .saveAsTable(target_table))

        else:        
            update_col = [col for col in df_touchpoints.columns if col != "etl_created_datetime"]
            update_set = {col: f"s.{col}" for col in update_col}
            target = DeltaTable.forName(spark, target_table)
            df_merge = (target.alias("t").merge(df_touchpoints.alias("s"),
                        "t.CONVERSATION_ID = s.CONVERSATION_ID")
                        .whenMatchedUpdate(set = update_set)
                        .whenNotMatchedInsertAll()
                        .execute())
        
        print("Taregt table updated")
        # Extract counts
        metrics = spark.sql(f"DESCRIBE HISTORY {target_table}").first()["operationMetrics"]
        inserted_rows = metrics.get("numOutputRows", 0)
        updated_rows = metrics.get("numTargetRowsUpdated", 0)
            
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_touchpoints.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,update_latest_tag
# add last_updated tag with current timestamp to identify table last refreshed datetime.
add_last_updated_tag(target_table)

# COMMAND ----------

# DBTITLE 1,view_creation
#view creation
catalog = f"transformation_{env}"
schema = "plt_customer360"
object_name = "vw_touchpoints"
select_query = c360_view_queries[object_name].format(catalog=catalog, schema=schema)
create_view_or_table(catalog=catalog, schema=schema, object_name=object_name, select_query=select_query)

# COMMAND ----------

# DBTITLE 1,plt_logging
# Prepare platinum table statistics dictionary for logging

plt_stats = {
    "gld_table": target_table,
    "dim_new_records_count": 0,
    "dim_updated_records_count": 0,
    "fact_inserted_records_count": inserted_rows,
    "fact_updated_records_count": updated_rows
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)
 

# COMMAND ----------

# DBTITLE 1,job_trigger
  trigger_databricks_job_by_name('workflow_orchestration')
