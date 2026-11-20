# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC This notebook summarizes the data pipeline for transforming Gold tables into the Platinum table `location_intelligence`. It outlines the source and target tables, schema enforcement, and load strategy.
# MAGIC 1. Source Gold Table: 
# MAGIC | Schema | Table |
# MAGIC | --- | --- |
# MAGIC |gld_ipera |dwh_fact_wifi_guest_activities |
# MAGIC |gld_ipera| dwh_dim_wifi_guest_details|
# MAGIC
# MAGIC 2. Target Platinum table: **location_intelligence**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 
# MAGIC
# MAGIC 4. Load Strategy: **Incremental Load**
# MAGIC

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
# dbutils.widgets.text("env","dev")
# env = dbutils.widgets.get("env")
# dbutils.widgets.text("current_job_run_id", "")
# current_job_run_id = dbutils.widgets.get("current_job_run_id")
# dbutils.widgets.text('start_date','')
# start_date = dbutils.widgets.get('start_date')
# dbutils.widgets.text('end_date','')
# end_date = dbutils.widgets.get('end_date')
# dbutils.widgets.text('buffer_days','')
# end_date = dbutils.widgets.get('buffer_days')

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include base fucntions
# MAGIC %run ../../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,include table config
# MAGIC %run ../../../config/table_config

# COMMAND ----------

# DBTITLE 1,include view ddl
# MAGIC %run ../../../config/view_queries

# COMMAND ----------

# DBTITLE 1,get notebook parameters
try:
    notebook_vars = [
        'env','start_date','end_date','current_job_run_id','buffer_days'
    ]
    env,start_date,end_date,current_job_run_id,buffer_days= get_notebook_params(*notebook_vars, verbose=True)
    buffer_days = int(buffer_days)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,import  dependencies
from pyspark.sql.functions import col, when, current_timestamp, trim, lower, lit, regexp_replace,regexp_extract, instr, split, row_number, to_date, date_format, upper,min,max,expr,coalesce,length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo


# COMMAND ----------

# DBTITLE 1,initialize bronze and silver tables
#source schema
ipera_schema = f"transformation_{env}.gld_ipera"
#source table
ipera_activities = f"{ipera_schema}.dwh_fact_wifi_guest_activities"
guest_details = f"{ipera_schema}.dwh_dim_wifi_guest_details"
ipera_lookup = f"{ipera_schema}.dwh_dim_lookup"

#scv account master
scv_account_master = f"transformation_{env}.plt_customer360.scv_account_master"

#target table
target_table = f"transformation_{env}.plt_customer360.location_intelligence"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")


print(f"Source table : {ipera_activities}")
print(f"Source table : {guest_details}")
print(f"Target table : {target_table}")
print(f"current_timestamp : {current_uae_timestamp}")


# COMMAND ----------

# DBTITLE 1,full or incremental
if (not spark.catalog.tableExists(target_table)) or (spark.sql(f"SELECT 1 FROM {target_table} WHERE CATEGORY = 'WIFI' LIMIT 1").count() == 0):
    print("Table not exists or WIFI data not present, Full Load")
    spark.table(ipera_activities).createOrReplaceTempView("ipera_activities")
elif start_date and end_date and start_date < end_date:
    print("Table exists, Incremental Load with date range")
    print(f"Filter : Batch_ID >= {start_date} and Batch_ID <= {end_date}")
    
    spark.table(ipera_activities).filter(
        (to_date(col("BATCH_ID").cast("string"), "yyyyMMddHHmmss") >= to_date(lit(start_date))) &
        (to_date(col("BATCH_ID").cast("string"), "yyyyMMddHHmmss") <= to_date(lit(end_date)))
    ).createOrReplaceTempView("ipera_activities")
else:
    print("Table exists, Incremental Load")
    
    # timestamp = spark.sql(f"select max(etl_updated_datetime) as max_loaded_at from {target_table} WHERE CATEGORY = 'WIFI' ").collect()[0]['max_loaded_at']
    
    #Buffer 10 days
    timestamp = (datetime.now(ZoneInfo("Asia/Dubai")) - timedelta(days=buffer_days)).strftime("%Y-%m-%d")
    
    print(f"Filter : Batch_ID >= {timestamp}")
    
    spark.table(ipera_activities).filter(
        to_date(col("BATCH_ID").cast("string"), "yyyyMMddHHmmss") >= to_date(lit(timestamp))
        ).createOrReplaceTempView("ipera_activities")

# COMMAND ----------

# DBTITLE 1,transformation
df_location_inteligence_wifi = spark.sql(f"""
 with ipera_base as (
    select 
      LAG(ACTIVITY_NAME) OVER (PARTITION BY GUEST_ID ORDER BY CREATED_AT) AS PREVIOUS_ACTIVITY,
      LAG(CREATED_AT) OVER (PARTITION BY GUEST_ID ORDER BY CREATED_AT) AS PREVIOUS_ACTIVITY_TS,
      LAG(LOCATION_ID) OVER (PARTITION BY GUEST_ID ORDER BY CREATED_AT) AS PREVIOUS_LOCATION_ID,
      LEAD(ACTIVITY_NAME) OVER (PARTITION BY GUEST_ID ORDER BY CREATED_AT) AS NEXT_ACTIVITY,
      LEAD(CREATED_AT) OVER (PARTITION BY GUEST_ID ORDER BY CREATED_AT) AS NEXT_ACTIVITY_TS
      ,GUEST_ID
      ,YAS_ID
      ,LOCATION_ID
      ,LOCATION_NAME
      ,CREATED_AT
      ,ACTIVITY_NAME
    FROM ipera_activities
    WHERE ACTIVITY_NAME IN ('Disconnected', 'Connected')
    AND GUEST_ID IS NOT NULL AND GUEST_ID != ''
    AND YAS_ID IS NOT NULL AND YAS_ID != ''
    AND lower(LOCATION_NAME) NOT IN ('test', '', 'cda')
  ),filtered AS (
  SELECT *
  FROM ipera_base
  WHERE NOT (ACTIVITY_NAME = 'Disconnected' AND PREVIOUS_ACTIVITY = 'Disconnected')
),session_logic as (
    select 
      (case
        when ACTIVITY_NAME = "Connected" and PREVIOUS_ACTIVITY = "Disconnected" then true
        when LOCATION_ID != PREVIOUS_LOCATION_ID then true
        when date(CREATED_AT) != date(PREVIOUS_ACTIVITY_TS) then true
        when PREVIOUS_ACTIVITY_TS is null then true
        when datediff(second, PREVIOUS_ACTIVITY_TS, CREATED_AT) > 10800 and ACTIVITY_NAME = "Connected" and PREVIOUS_ACTIVITY = "Connected" then true
        else false end) as FLIP_RANK
      ,*
    from filtered
  ),ipera_activities_session_bins as (
  SELECT 
    SUM(CASE WHEN FLIP_RANK THEN 1 ELSE 0 END) OVER (PARTITION BY GUEST_ID ORDER BY CREATED_AT) AS SESSION_ID
    ,*
  FROM 
    session_logic
  ),corrected_end_time_ipera_sessions as (
    select
      GUEST_ID
      ,YAS_ID
      ,LOCATION_ID
      ,LOCATION_NAME
      ,SESSION_ID
      ,min(case 
     when ACTIVITY_NAME = "Connected" then CREATED_AT
     else null end) as START_TIME_raw
     , max(case 
     when ACTIVITY_NAME = "Disconnected" then CREATED_AT
     else null end) as end_time_raw
--   , ifnull(START_TIME_raw,  to_timestamp(date(START_TIME_raw))  )  as START_TIME
     ,ifnull(START_TIME_raw,  to_timestamp(date(end_time_raw))  )  as START_TIME
     ,end_time_raw as end_time
    from ipera_activities_session_bins
    group by 1,2,3,4,5
  ),location_intelligence_ipera_sessions as(
  select
    *
    ,datediff(SECOND, START_TIME, END_TIME) as SESSION_DURATION_SEC
  from corrected_end_time_ipera_sessions
  )
  select 
    {yas_id_standardization("liis.YAS_ID")} as yas_id
    ,"WIFI" as CATEGORY
    ,(case when l.CLEAN_LOOKUP_NAME is not null then l.CLEAN_LOOKUP_NAME else liis.LOCATION_NAME end) as ASSET_NAME
    ,liis.START_TIME as CHECKIN_TIMESTAMP
    ,liis.END_TIME as CHECKOUT_TIMESTAMP
    ,null as ANNUAL_PASS_FLAG
    ,null as PRODUCT_NAME
    ,map(
        "guest_id", STRING(liis.GUEST_ID),
        "email", gd.email,
        "yas_id", STRING(liis.YAS_ID),
        "location_id", STRING(liis.LOCATION_ID),
        "location_name",STRING(liis.LOCATION_NAME),
        "day_session_id",STRING(row_number() over (partition by liis.GUEST_ID,liis.YAS_ID,liis.LOCATION_ID,date(liis.START_TIME) order by liis.START_TIME asc))
     ) as OTHERS
  from location_intelligence_ipera_sessions liis
--   join with guest to get email id
  left join ( select ID,email from  {guest_details} 
  qualify row_number() over(partition by ID order by created_at desc) = 1
  ) gd on liis.GUEST_ID = gd.ID
  left join {ipera_lookup} l on liis.LOCATION_ID = l.LOOK_UP_CODE                       
                       
""")
df_location_inteligence_wifi = map_asset_category(df_location_inteligence_wifi,"ASSET_NAME")
df_location_inteligence_wifi.createOrReplaceTempView("ipera_wifi")

# COMMAND ----------

# DBTITLE 1,Final Data product
df_location_inteligence_wifi = spark.sql(f"""
    with ipera_raw as (
        select 
            NULL as purchaser_omni_sk,
            NULL as purchaser_scv_sk,
            NULL as owner_omni_sk,
            scv.scv_account_sk as owner_scv_sk,
            scv.email_id as scv_email_id,
            NULL AS fl_scv_sk,
            c.CATEGORY,
            c.ASSET_NAME,
            cast(c.CHECKIN_TIMESTAMP as timestamp) as CHECKIN_TIMESTAMP,
            cast(c.CHECKOUT_TIMESTAMP as timestamp) as CHECKOUT_TIMESTAMP,
            c.ANNUAL_PASS_FLAG,
            c.PRODUCT_NAME,
            '{current_uae_timestamp}' as LOADED_AT,
            c.OTHERS
        from ipera_wifi c
        left join {scv_account_master} scv 
            on c.yas_id = scv.yas_id 
            and scv.active_yas_id_flag = 'Y' -- need confirmation
    )
    select
        map('scv_sk',cast(i.owner_scv_sk as string),
          'omni_sk',NULL,
          'email_id', cast( coalesce(scv_email_id,others.email) as string)
          ) as entity_identifiers,
        i.purchaser_scv_sk,
        i.purchaser_omni_sk,
        i.owner_scv_sk,
        i.owner_omni_sk,
        i.fl_scv_sk,
        i.category,
        i.asset_name,
        i.checkin_timestamp,
        i.checkout_timestamp,
        i.annual_pass_flag,
        i.product_name,
        case 
            when i.purchaser_scv_sk is null 
                and i.purchaser_omni_sk is null 
                and i.owner_scv_sk is null 
                and i.fl_scv_sk is null
                and i.owner_omni_sk is null 
            then 'N'
            else 'Y' 
        end as valid_sk,  -- all sk are null then N else Y
        'IPERA' AS source_name,
        '{current_uae_timestamp}' as etl_created_datetime,
        '{current_uae_timestamp}' as etl_updated_datetime,
        i.others
    from ipera_raw i
""")

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_location_inteligence_wifi = apply_schema_casting(df_location_inteligence_wifi,location_inteligence_schema)   

# COMMAND ----------

# DBTITLE 1,load to UC
# Build DDL string from schema dict
location_inteligence_dll = ",\n".join(
    f"{col} {dtype}" for col, dtype in location_inteligence_schema.items()
)
 
try:
    # Convert DDL string to StructType for schema enforcement
    ddl_struct = StructType.fromDDL(location_inteligence_dll)
    # Compare actual DataFrame schema with expected schema (lenient)
    if compare_schema_lenient(df_location_inteligence_wifi.schema, ddl_struct):
        print("Schema matches!")

        inserted_count = 0
        updated_count = 0

         # Check table state
        table_exists = spark.catalog.tableExists(target_table)
        has_facepass_data = table_exists and spark.sql(f"SELECT 1 FROM {target_table} WHERE CATEGORY = 'WIFI' LIMIT 1").count() > 0

        #Full Load
        if has_facepass_data:
            print("Incremental Load")
             # Upsert df_location_inteligence_wifi into target_table based on GUEST_ID
            delta_table = DeltaTable.forName(spark, target_table)

            merge_condition = """
                            t.category = 'WIFI'
                            AND t.others['guest_id'] = s.others['guest_id']
                            AND t.others['yas_id'] = s.others['yas_id'] 
                            AND t.others['location_id'] = s.others['location_id']
                            AND t.checkin_timestamp = s.checkin_timestamp
                        """
            df_result = delta_table.alias("t").merge(
                df_location_inteligence_wifi.alias("s"),
                merge_condition
            ).whenMatchedUpdate(
                set={
                    "etl_updated_datetime": f"'{current_uae_timestamp}'",
                    "checkout_timestamp": "s.checkout_timestamp"
                }
            ).whenNotMatchedInsertAll().execute()

            result_row = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count = result_row["num_updated_rows"]
        else:
            print("Full Load")

            if not table_exists:
                try:
                    (spark.createDataFrame([], df_location_inteligence_wifi.schema)
                        .write.format("delta")
                        .saveAsTable(target_table))
                except Exception:
                    print("Table was created concurrently -- appending instead.")
                    pass

            # Single data write — always exactly one append, no partitionBy needed
            (df_location_inteligence_wifi
                .write.format("delta")
                .mode("append")
                .saveAsTable(target_table))
       
            # Get inserted row count from Delta table history — avoids double computation
            history_metrics = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").select("operationMetrics").collect()[0][0]
            inserted_count = int(history_metrics.get("numOutputRows", 0))

        print("Location Intlligence Wifi Data Loaded")
        print(f"Rows Inserted: {inserted_count}")
        print(f"Rows Updated: {updated_count}")
    else:
        # Handle schema mismatch
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_location_inteligence_wifi.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    # Exception handling for notebook failure
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,tbl readiness - update tag
readiness_table = f"transformation_{env}.plt_customer360.readiness_location_intelligence_wifi"

df = spark.createDataFrame([(spark.sql("SELECT from_utc_timestamp(current_timestamp(), 'Asia/Dubai')").collect()[0][0],)],["last_updated_timestamp"])
display(df)

df.write.mode("append").saveAsTable(readiness_table)
add_last_updated_tag(readiness_table)

# COMMAND ----------

# DBTITLE 1,trigger orchestration
trigger_databricks_job_by_name('workflow_orchestration')

# COMMAND ----------

# DBTITLE 1,optimize table
try:
    if 'location_intelligence' in c360_table_config:
        print("\nOptimizing Table before view creation...")
        config = c360_table_config['location_intelligence']
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

# COMMAND ----------

# DBTITLE 1,view create
try:
    catalog = f"transformation_{env}"
    schema = "plt_customer360"
    object_names = ["vw_location_intelligence", "vw_location_intelligence_o2o"]
    for object_name in object_names:
        select_query = c360_view_queries[object_name].format(catalog=catalog, schema=schema)
        create_view_or_table(catalog=catalog, schema=schema, object_name=object_name, select_query=select_query)
        print(f"View '{object_name}' created successfully.")
except Exception as e:
    print(f"Error creating view: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,log stats
# Prepare platinum table statistics dictionary for logging

plt_stats = {
    "gld_table": target_table,
    "dim_new_records_count": 0,
    "dim_updated_records_count": 0,
    "fact_inserted_records_count": inserted_count,
    "fact_updated_records_count": updated_count
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)
 
