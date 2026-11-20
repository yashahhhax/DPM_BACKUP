# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC **The `marketing` table serves as the consolidated platinum-level marketing data product, integrating Email and Push Notification flows from Emarsys gold tables. It enforces schema consistency and supports full load, incremental and date range load strategies.**
# MAGIC
# MAGIC 1. Source Tables:
# MAGIC
# MAGIC  Schema | Table | Purpose |
# MAGIC  --- | --- | --- |
# MAGIC  gld_emarsys | dwh_fact_email | Email flow — CURRENT_FLAG = 'Y' |
# MAGIC  gld_emarsys | dwh_fact_push_notification | Push notification flow |
# MAGIC  gld_emarsys | dwh_dim_contact | Customer details — join on USER_PK |
# MAGIC  gld_emarsys | dwh_dim_emarsys_account | Account name — join on ACCOUNT_EMARSYS_KEY |
# MAGIC  gld_emarsys | dwh_dim_email_campaign | Email campaign details — join on EMAIL_CAMPAIGN_KEY |
# MAGIC  gld_emarsys | dwh_dim_push_campaign | Push campaign details — join on PUSH_CAMPAIGN_KEY |
# MAGIC
# MAGIC 2. Target Platinum table: **transformation_dev.plt_customer360.marketing**
# MAGIC
# MAGIC 3. Load Strategy: **Full Load (Run 1) | Incremental (Run 2: MERGE on fact_key + source, watermark - 7 days buffer) | Date Range (start_date and end_date provided)**
# MAGIC
# MAGIC 4. Merge Key: **fact_key + campaign_source**
# MAGIC
# MAGIC 5. Watermark column: **MODIFIED_DATETIME (source timestamp) — 7 day buffer applied**
# MAGIC
# MAGIC 6. Pattern: **UNION of Email flow + Push flow**
# MAGIC
# MAGIC 7. Schema Enforcement implemented

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
# dbutils.widgets.text("env","dev")
# dbutils.widgets.text("start_date","")
# dbutils.widgets.text("end_date","")
# dbutils.widgets.text("current_job_run_id","")
# dbutils.widgets.text("buffer_days","7")

# env                = dbutils.widgets.get("env")
# start_date         = dbutils.widgets.get("start_date")
# end_date           = dbutils.widgets.get("end_date")
# current_job_run_id = dbutils.widgets.get("current_job_run_id")
# buffer_days        = int(dbutils.widgets.get("buffer_days"))

# print(f"env                : {env}")
# print(f"start_date         : {start_date}")
# print(f"end_date           : {end_date}")
# print(f"current_job_run_id : {current_job_run_id}")
# print(f"buffer_days        : {buffer_days}")

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include base fucntions
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %run ../../config/table_config

# COMMAND ----------

# DBTITLE 1,include view ddl
# MAGIC %run ../../config/view_queries

# COMMAND ----------

# DBTITLE 1,get notebook parameters
try:
    notebook_vars = [
        'env', 'start_date', 'end_date', 'current_job_run_id', 'buffer_days'
    ]
    env, start_date, end_date, current_job_run_id, buffer_days  = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,import  dependencies
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit
from zoneinfo import ZoneInfo
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,initialize source tables
# source schemas
src_emarsys_db = f"transformation_{env}.gld_emarsys"
tgt_db         = f"transformation_{env}.plt_customer360"

# source tables — Email flow
src_fact_email         = f"{src_emarsys_db}.dwh_fact_email"
src_dim_contact        = f"{src_emarsys_db}.dwh_dim_contact"
src_dim_emarsys_acc    = f"{src_emarsys_db}.dwh_dim_emarsys_account"
src_dim_email_campaign = f"{src_emarsys_db}.dwh_dim_email_campaign"

# source tables — Push flow
src_fact_push          = f"{src_emarsys_db}.dwh_fact_push_notification"
src_dim_push_campaign  = f"{src_emarsys_db}.dwh_dim_push_campaign"

#scv master
scv_account_master = f"transformation_{env}.plt_customer360.scv_account_master"

# target table
target_table = f"transformation_{env}.plt_customer360.marketing"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table       : {src_fact_email}")
print(f"Source table       : {src_fact_push}")
print(f"Source table       : {src_dim_contact}")
print(f"Source table       : {src_dim_emarsys_acc}")
print(f"Source table       : {src_dim_email_campaign}")
print(f"Source table       : {src_dim_push_campaign}")
print(f"Source table       : {scv_account_master}")
print(f"Target table       : {target_table}")
print(f"UAE timestamp      : {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,filter condition
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
    filter_condition = f"and DATE(MODIFIED_DATETIME) between '{start_date}' and '{end_date}'"
elif is_full_load:
    filter_condition = ''
else:
    filter_condition = f"and MODIFIED_DATETIME > (select coalesce(max(etl_updated_datetime) - INTERVAL {buffer_days} days, '1900-01-01') from {target_table})"

print(f"Load mode         : {'DATE RANGE' if use_date_range else 'FULL LOAD' if is_full_load else 'INCREMENTAL'}")
print(f"Watermark         : {watermark}")
print(f"Filter condition  : {filter_condition}")


# COMMAND ----------

# DBTITLE 1,email fact raw scan
spark.sql(f"""
            --------------------scan email fact table once--------------------
            select
                fe.EMAIL_SK                                         as fact_key
                ,fe.SOURCE                                          as source
                ,fe.USER_PK
                ,fe.MESSAGE_ID
                ,fe.EVENT_PLATFORM
                ,fe.UNSUBSCRIBE_FLAG
                ,fe.EMAIL_CANCEL_REASON
                ,fe.BOUNCE_TYPE
                ,fe.ACCOUNT_EMARSYS_KEY
                ,fe.EVENT_TYPE
                ,fe.EVENT_TIME
                ,fe.OPEN_CLICK_IS_MOBILE
                ,fe.DATE_KEY
                ,fe.LAUNCH_KEY
                ,fe.EMAIL_CAMPAIGN_KEY
            from {src_fact_email} fe
            where fe.CURRENT_FLAG = 'Y'
              {filter_condition}
            qualify(row_number() over (partition by fact_key,source order by MODIFIED_DATETIME desc)) = 1
""").createOrReplaceTempView("email_fact_raw")

# COMMAND ----------

# DBTITLE 1,push fact raw scan
spark.sql(f"""
            --------------------scan push fact table once--------------------
            select
                fp.PUSH_SK                                          as fact_key
                ,fp.SOURCE                                          as source
                ,fp.USER_PK
                ,fp.ACCOUNT_EMARSYS_KEY
                ,fp.PLATFORM
                ,fp.PUSH_NOT_SENT_REASON
                ,fp.EVENT_TYPE
                ,fp.EVENT_TIME
                ,fp.DATE_KEY
                ,fp.PUSH_CAMPAIGN_KEY
                ,fp.MODIFIED_DATETIME                              as modified_datetime
            from {src_fact_push} fp
            where 1=1
            {filter_condition}
            qualify(row_number() over (partition by fp.PUSH_SK, fp.SOURCE order by fp.MODIFIED_DATETIME desc)) = 1
""").createOrReplaceTempView("push_fact_raw")

# COMMAND ----------

# DBTITLE 1,dim tables raw scan
#--------------------scan all dim tables once — reused by both email and push flows--------------------
spark.sql(f"""
            select USER_PK, EMAIL, OPT_IN,
                   REGISTRATION_LANGUAGE, DATE_OF_REGISTRATION, first_name, last_name, dob, gender, mobile, opt_in
            from   {src_dim_contact}
            where  CURRENT_FLAG = 'Y'
""").createOrReplaceTempView("dim_contact")

spark.sql(f"""
            select ACCOUNT_EMARSYS_KEY, ACCOUNT_NAME
            from   {src_dim_emarsys_acc}
            where  CURRENT_FLAG = 'Y'
""").createOrReplaceTempView("dim_emarsys_acc")

spark.sql(f"""
            select EMAIL_CAMPAIGN_KEY, CAMPAIGN_NAME, UTM_TAG,
                   CAMPAIGN_TYPE, CATEGORY, LAUNCH_DATE
            from   {src_dim_email_campaign}
            where  CURRENT_FLAG = 'Y'
""").createOrReplaceTempView("dim_email_campaign")

spark.sql(f"""
            --------------------dedup on PUSH_CAMPAIGN_KEY — CURRENT_FLAG='Y' is not sufficient--------------------
            select PUSH_CAMPAIGN_KEY, NAME, SOURCE, CREATED_AT,
                   SEGMENT_ID, STATUS, PUSH_INTERNAL_CAMPAIGN_ID
            from (
                select PUSH_CAMPAIGN_KEY, NAME, SOURCE, CREATED_AT,
                       SEGMENT_ID, STATUS, PUSH_INTERNAL_CAMPAIGN_ID,
                       ROW_NUMBER() OVER (
                           PARTITION BY PUSH_CAMPAIGN_KEY
                           ORDER BY CREATED_AT DESC, LOADED_AT DESC
                       ) as rn
                from   {src_dim_push_campaign}
                where  CURRENT_FLAG = 'Y'
            ) where rn = 1
""").createOrReplaceTempView("dim_push_campaign")

# COMMAND ----------

# DBTITLE 1,scv dedup
spark.sql(f"""
    SELECT scv_account_sk, email_id
    FROM {scv_account_master}
    WHERE source_name = 't_mdm_guest'
    QUALIFY row_number() over(partition by email_id order by modified_date desc) = 1
""").createOrReplaceTempView("vw_scv_dedup")

# COMMAND ----------

# DBTITLE 1,email flow transformation
df_email = spark.sql(f"""
            --------------------Email flow — uses filtered email_fact_raw and dim tempviews--------------------
            select
                map('scv_sk',cast(scv.scv_account_sk as string),
                  'omni_sk',NULL,
                  'email_id', {email_standardization("c.EMAIL")}
                  )                                                        as entity_identifiers
                ,scv.scv_account_sk                                        as owner_scv_sk
                ,cast(null as string)                                      as owner_omni_sk
                ,fe.fact_key                                               as fact_key
                ,{email_standardization("c.EMAIL")}                        as customer_email
                ,fe.EVENT_TYPE                                             as event_type
                ,fe.EVENT_TIME                                             as event_time
                ,CASE WHEN fe.EMAIL_CANCEL_REASON IS NOT NULL
                      THEN fe.EMAIL_CANCEL_REASON
                      ELSE fe.BOUNCE_TYPE
                 END                                                       as event_sub_type
                ,'Emarsys-Email'                                           as campaign_source
                ,fe.MESSAGE_ID                                             as message_id
                ,fe.EVENT_PLATFORM                                         as hardware
                ,fe.UNSUBSCRIBE_FLAG                                       as unsubscribe_flag
                ,acc.ACCOUNT_NAME                                          as account_name
                ,fe.EMAIL_CAMPAIGN_KEY                                     as campaign_key
                ,ec.CAMPAIGN_NAME                                          as campaign_name
                ,ec.UTM_TAG                                                as utm_tag
                ,ec.CAMPAIGN_TYPE                                          as campaign_type
                ,ec.CATEGORY                                               as campaign_category
                ,ec.LAUNCH_DATE                                            as campaign_launch_date
                ,map(
                    'open_click_is_mobile',  cast(fe.OPEN_CLICK_IS_MOBILE  as string),
                    'date_key',              cast(fe.DATE_KEY              as string),
                    'emarsys_customer_id',   cast(fe.USER_PK               as string),
                    'launch_key',            cast(fe.LAUNCH_KEY            as string),
                    'email_id',              cast(c.EMAIL                  as string),
                    'first_name',            cast(c.FIRST_NAME             as string),
                    'last_name',             cast(c.LAST_NAME              as string),
                    'opt_in',               cast(c.OPT_IN                  as string),
                    'mobile',               cast(c.MOBILE                  as string),
                    'gender',               cast(c.GENDER                  as string),
                    'dob',                  cast(c.DOB                     as string),
                    'registration_language',cast(c.REGISTRATION_LANGUAGE   as string),
                    'date_of_registration', cast(c.DATE_OF_REGISTRATION    as string)
                )                                                          as others
            from email_fact_raw fe
            inner join dim_contact        c   on fe.USER_PK            = c.USER_PK
            inner join dim_emarsys_acc    acc on fe.ACCOUNT_EMARSYS_KEY = acc.ACCOUNT_EMARSYS_KEY
            inner join dim_email_campaign ec  on fe.EMAIL_CAMPAIGN_KEY  = ec.EMAIL_CAMPAIGN_KEY
            left join vw_scv_dedup        scv ON scv.email_id = {email_standardization("c.EMAIL")}
""")

# COMMAND ----------

# DBTITLE 1,push flow transformation
df_push = spark.sql(f"""
            --------------------Push flow — uses filtered push_fact_raw and dim tempviews--------------------
            select
                map('scv_sk',cast(scv.scv_account_sk as string),
                  'omni_sk',NULL,
                  'email_id', {email_standardization("c.EMAIL")}
                  )                                                              as entity_identifiers
                ,scv.scv_account_sk                                              as owner_scv_sk
                ,cast(null as string)                                            as owner_omni_sk
                ,fp.fact_key                                                     as fact_key
                ,{email_standardization("c.EMAIL")}                              as customer_email
                ,fp.EVENT_TYPE                                                   as event_type
                ,fp.EVENT_TIME                                                   as event_time
                ,fp.PUSH_NOT_SENT_REASON                                         as event_sub_type
                ,'Emarsys-PushNotification'                                      as campaign_source
                ,cast(null as int)                                               as message_id
                ,fp.PLATFORM                                                     as hardware
                ,cast(null as boolean)                                           as unsubscribe_flag
                ,acc.ACCOUNT_NAME                                                as account_name
                ,fp.PUSH_CAMPAIGN_KEY                                            as campaign_key
                ,pc.NAME                                                         as campaign_name
                ,cast(null as string)                                            as utm_tag
                ,pc.SOURCE                                                       as campaign_type
                ,cast(null as string)                                            as campaign_category
                ,pc.CREATED_AT                                                   as campaign_launch_date
                ,map(
                    'date_key',              cast(fp.DATE_KEY                    as string),
                    'campaign_segment_id',   cast(pc.SEGMENT_ID                  as string),
                    'campaign_status',       cast(pc.STATUS                      as string),
                    'campaign_internal_id',  cast(pc.PUSH_INTERNAL_CAMPAIGN_ID   as string),
                    'emarsys_customer_id',   cast(fp.USER_PK                     as string),
                    'email_id',              cast(c.EMAIL                        as string),
                    'first_name',            cast(c.FIRST_NAME                   as string),
                    'last_name',             cast(c.LAST_NAME                    as string),
                    'opt_in',               cast(c.OPT_IN                        as string),
                    'mobile',               cast(c.MOBILE                        as string),
                    'gender',               cast(c.GENDER                        as string),
                    'dob',                  cast(c.DOB                           as string),
                    'registration_language',cast(c.REGISTRATION_LANGUAGE         as string),
                    'date_of_registration', cast(c.DATE_OF_REGISTRATION          as string)
                )                                                        as others
            from push_fact_raw fp
            inner join dim_contact       c   on fp.USER_PK            = c.USER_PK
            inner join dim_emarsys_acc   acc on fp.ACCOUNT_EMARSYS_KEY = acc.ACCOUNT_EMARSYS_KEY
            inner join dim_push_campaign pc  on fp.PUSH_CAMPAIGN_KEY   = pc.PUSH_CAMPAIGN_KEY
            left join vw_scv_dedup       scv ON scv.email_id = {email_standardization("c.EMAIL")}
""")

# COMMAND ----------

# DBTITLE 1,union email and push flows
#--------------------UNION email + push flows--------------------
df_marketing = df_email.unionByName(df_push)

# COMMAND ----------

# MAGIC %md
# MAGIC Schema casting

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_marketing = apply_schema_casting(df_marketing, marketing_schema)

# COMMAND ----------

# DBTITLE 1,write to UC
# Build DDL string from schema dict
marketing_ddl = ",\n".join(
    f"{col_name} {dtype}" for col_name, dtype in marketing_schema.items()
)

try:
    ddl_struct = StructType.fromDDL(marketing_ddl)
    df_marketing = df_marketing \
        .withColumn("source_name",          lit("EMARSYS")) \
        .withColumn("etl_created_datetime", lit(current_uae_timestamp).cast("timestamp")) \
        .withColumn("etl_updated_datetime", lit(current_uae_timestamp).cast("timestamp"))

    if compare_schema_lenient(df_marketing.schema, ddl_struct):
        print("Schema matches!")

        # Check if table exists
        inserted_count = 0
        updated_count  = 0
        table_exists = spark.catalog.tableExists(target_table)

        if use_date_range:
            print(f"Load mode  : DATE RANGE ({start_date} to {end_date})")
        elif is_full_load:
            print(f"Load mode  : FULL LOAD")
        else:
            print(f"Load mode  : INCREMENTAL (watermark: {watermark})")

        if is_full_load:
            #--------------------full load — TRUNCATE + INSERT--------------------
            if table_exists:
                spark.sql(f"TRUNCATE TABLE {target_table}")
            (df_marketing.write.format("delta").mode("append").saveAsTable(target_table))
            inserted_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
            updated_count  = 0
        else:
            #------------incremental / date range — MERGE on fact_key + campaign source---------
            #------------selective update — etl_created_datetime preserved on match--------------------
            #------------update_set built dynamically from schema — excludes merge keys + etl_created_datetime--------------------
            exclude_cols = {"fact_key", "campaign_source", "etl_created_datetime"}
            update_set   = {col: f"src.{col}" for col in marketing_schema.keys() if col not in exclude_cols}
            delta_tgt    = DeltaTable.forName(spark, target_table)
            df_result = (delta_tgt.alias("tgt")
                .merge(df_marketing.alias("src"),
                       "tgt.fact_key = src.fact_key AND tgt.campaign_source = src.campaign_source")
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsertAll()
                .execute())
            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

        print(f"Marketing Table Loaded...")
        print(f"Rows Inserted : {inserted_count}")
        print(f"Rows Updated  : {updated_count}")
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_marketing.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,update_latest_tag
#Table optimization
try:
    if 'marketing' in c360_table_config:
        print("\nOptimizing Table before view creation...")
        config = c360_table_config['marketing']
        print(f"Target Table Reference: {target_table}")
        cluster_key = config["cluster_by"]
        print(f"Cluster By: {cluster_key}")

        existing_cluster_keys = spark.sql(f"DESCRIBE DETAIL {target_table}").select("clusteringColumns").collect()[0][0]
        #existing cluster keys match with config then skip ALTER TABLE
        if existing_cluster_keys and set(existing_cluster_keys) == set([k.strip() for k in cluster_key.split(',')]):
            print("Cluster keys already match. Skipping ALTER TABLE.")
        else:
            spark.sql(f"ALTER TABLE {target_table} CLUSTER BY ({cluster_key})")

        spark.sql(f"OPTIMIZE {target_table}")
    else:
        print(f"Table '{table_name}' not found in c360_table_config. Skipping optimization.")
except Exception as e:
    print(f"Error optimizing table: {str(e)}")
    raise e

#view creation
catalog     = f"transformation_{env}"
schema      = "plt_customer360"
object_name = "vw_marketing"
select_query = c360_view_queries[object_name].format(catalog=catalog, schema=schema)
create_view_or_table(catalog=catalog, schema=schema, object_name=object_name, select_query=select_query)

#updated tag
add_last_updated_tag(target_table)

# COMMAND ----------

# DBTITLE 1,plt_logging
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

# DBTITLE 1,trigger workflow
trigger_databricks_job_by_name('workflow_orchestration')
