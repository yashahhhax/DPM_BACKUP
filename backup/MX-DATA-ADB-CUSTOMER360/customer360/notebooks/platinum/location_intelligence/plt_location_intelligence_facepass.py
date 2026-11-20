# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC This notebook summarizes the data pipeline for transferring portrait information from the gold table to the platinum location_intelligence table. It outlines the source and target tables, schema enforcement, and load strategy.
# MAGIC 1. Source Gold Table: 
# MAGIC | Schema | Table |
# MAGIC | --- | --- |
# MAGIC |gld_facepassreporting |gld_lkp_portrait_info |
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

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include table config
# MAGIC %run ../../../config/table_config

# COMMAND ----------

# DBTITLE 1,include base fucntions
# MAGIC %run ../../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,include view queries
# MAGIC %run ../../../config/view_queries

# COMMAND ----------

# DBTITLE 1,get notebook parameters
try:
    notebook_vars = [
        'env','start_date','end_date','current_job_run_id'
    ]
    env,start_date,end_date,current_job_run_id= get_notebook_params(*notebook_vars, verbose=True)
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
#source table 
facepass = f"transformation_{env}.gld_facepassreporting.gld_lkp_portrait_info"

#target table
target_table = f"transformation_{env}.plt_customer360.location_intelligence"
#scv master
scv_account_master = f"transformation_{env}.plt_customer360.scv_account_master"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")
print(f"Source Table: {facepass}")
print(f"Target Table: {target_table}")

print(f"current_uae_timestamp: {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,filter
if (not spark.catalog.tableExists(target_table)) or (spark.sql(f"SELECT 1 FROM {target_table} WHERE CATEGORY = 'FRICTIONLESS' LIMIT 1").count() == 0):
    print("Table not exists or FACEPASS/FACETAG data not present, Full Load")
    facepass_filter = "1=1"
    
elif start_date and end_date and start_date < end_date:
    print("Table exists, Incremental Load with date range")
    facepass_filter = f"cast(LOADED_AT as date) BETWEEN '{start_date}' and  '{end_date}'"
    
else:
    print("Table exists, Incremental Load")
    timestamp = spark.sql(f"select cast(max(etl_updated_datetime) as date) as max_loaded_at from {target_table} WHERE  CATEGORY = 'FRICTIONLESS' ").collect()[0]['max_loaded_at']

    facepass_filter = f"cast(LOADED_AT as date) >= '{timestamp}'"
#cehck update time 
print(f"Facepass Filter: {facepass_filter}")


# COMMAND ----------

# MAGIC %md
# MAGIC Union Tickets from Hotels

# COMMAND ----------

# DBTITLE 1,Transformation
df_location_inteligence_facepass = spark.sql(f"""
   -- Clean and extract yas_id from out_biz_id
   with cleaned_facepass as (
        select
            {yas_id_standardization("out_biz_id")} AS yas_id
            ,META_INFO:birthDay AS dob
            ,META_INFO:employeeId AS employeeId
            ,META_INFO:ticketReferenceNumber AS mediaidentifier
            ,*
        from {facepass}
        where {facepass_filter}
        -- just to make sure portrait ID is unique
        qualify row_number() over (partition by PORTRAIT_ID order by ifnull(update_time,create_time) desc) = 1
   )
    select 
        map('scv_sk',cast(scv.scv_account_sk as string),
           'omni_sk',NULL,
           'email_id', cast(scv.email_id as string)
           ) as entity_identifiers
        ,NULL as purchaser_omni_sk
        ,NULL as purchaser_scv_sk
        ,NULl as owner_omni_sk
        ,scv.scv_account_sk as owner_scv_sk
        ,scv.scv_account_sk as fl_scv_sk
        ,'FRICTIONLESS' as CATEGORY
        ,null as ASSET_NAME
        ,greatest(fp.create_time, fp.update_time) as CHECKIN_TIMESTAMP
        ,NULL as CHECKOUT_TIMESTAMP
        ,null as ANNUAL_PASS_FLAG
        ,null as PRODUCT_NAME
        ,"FACEPASS_PROFILE_CREATION" as SOURCE
        ,map(
            'portrait_id', string(PORTRAIT_ID),
            'yas_id', fp.yas_id,
            'parent_yas_id', split(fp.out_biz_id, "_")[0],
            'name', fp.name,
            'dob',string(fp.dob),
            'employeeid',string(fp.employeeId),
            'create_time', string(fp.create_time),
            'update_time', string(fp.update_time),
            'image_md5', fp.image_md5
        ) as OTHERS
   from cleaned_facepass fp
   left join {scv_account_master} scv 
        on fp.yas_id = scv.yas_id 
        and scv.active_yas_id_flag = 'Y'
   where fp.name not like '%test%'
   AND fp.yas_id is not null

   union all

    select 
        map('scv_sk',cast(scv.scv_account_sk as string),
           'omni_sk',NULL,
           'email_id', cast(scv.email_id as string)
           ) as entity_identifiers
        ,NULL as purchaser_omni_sk
        ,NULL as purchaser_scv_sk
        ,NULl as owner_omni_sk
        ,scv.scv_account_sk as owner_scv_sk
        ,scv.scv_account_sk as fl_scv_sk
        ,'FRICTIONLESS' as CATEGORY
        ,null as ASSET_NAME
        ,greatest(fp.create_time, fp.update_time) as CHECKIN_TIMESTAMP
        ,NULL as CHECKOUT_TIMESTAMP
        ,null as ANNUAL_PASS_FLAG
        ,null as PRODUCT_NAME
        ,"FACETAG_PROFILE_CREATION" as SOURCE
        ,map(
            'portrait_id', string(PORTRAIT_ID),
            'yas_id', fp.out_biz_id,
            'parent_yas_id', scv.parent_yas_id,
            'facetag_media_identifier', string(fp.mediaidentifier),
            'name', fp.name,
            'dob',string(fp.dob),
            'employeeid',string(fp.employeeId),
            'create_time', string(fp.create_time),
            'update_time', string(fp.update_time),
            'image_md5', fp.image_md5
        ) as OTHERS
   from cleaned_facepass fp
   left join {scv_account_master} scv 
        on fp.out_biz_id = scv.yas_id 
        and scv.source_name = 'frictionless_face_tag'
   where fp.name not like '%test%'
   and fp.name != 'FaceCapture'
   AND fp.out_biz_id not like '%-%'
   AND length(fp.out_biz_id) between 25 and 40
""")

df_location_inteligence_facepass = map_asset_category(df_location_inteligence_facepass,"ASSET_NAME")

df_location_inteligence_facepass.createOrReplaceTempView("facepass_raw")

# COMMAND ----------

# DBTITLE 1,final df
df_location_inteligence_facepass = spark.sql(f"""
select 
  f.entity_identifiers
  ,f.purchaser_scv_sk
  ,f.purchaser_omni_sk 
  ,f.owner_scv_sk
  ,f.owner_omni_sk 
  ,f.fl_scv_sk
  ,f.category
  ,f.asset_name
  ,f.checkin_timestamp
  ,f.checkout_timestamp
  ,f.annual_pass_flag
  ,f.product_name
  ,case when f.purchaser_scv_sk is null and
            f.purchaser_omni_sk is null and
            f.owner_scv_sk is null and
            f.fl_scv_sk is null and
            f.owner_omni_sk is null then 'N'
            else 'Y' end as valid_sk  -- all sk are null then N else Y
  ,f.SOURCE AS source_name
  , '{current_uae_timestamp}' as etl_created_datetime
  , '{current_uae_timestamp}' as etl_updated_datetime
  ,f.others
  from facepass_raw f
  """)

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_location_inteligence_facepass = apply_schema_casting(df_location_inteligence_facepass,location_inteligence_schema )


# COMMAND ----------

# DBTITLE 1,Write to UC
# Build DDL string from schema dict
location_inteligence_dll = ",\n".join(
    f"{col} {dtype}" for col, dtype in location_inteligence_schema.items()
)
 
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(location_inteligence_dll)
    if compare_schema_lenient(df_location_inteligence_facepass.schema, ddl_struct):
        print("Schema matches!")

        inserted_count = 0
        updated_count = 0

        # Check table state
        table_exists = spark.catalog.tableExists(target_table)
        has_facepass_data = table_exists and spark.sql(f"SELECT 1 FROM {target_table} WHERE CATEGORY = 'FRICTIONLESS' LIMIT 1").count() > 0

        #Full Load
        if has_facepass_data:
            print("Incremental Load")
            delta_table = DeltaTable.forName(spark, target_table)

            df_result = delta_table.alias("target").merge(
                df_location_inteligence_facepass.alias("source"),
                """target.others['portrait_id'] = source.others['portrait_id']
                    AND target.category = 'FRICTIONLESS'
                """
            ).whenMatchedUpdate(
                set={
                    "purchaser_scv_sk": "source.purchaser_scv_sk",
                    "purchaser_omni_sk": "source.purchaser_omni_sk",
                    "owner_scv_sk": "source.owner_scv_sk",
                    "owner_omni_sk": "source.owner_omni_sk",
                    "asset_name": "source.asset_name",
                    "checkin_timestamp": "source.checkin_timestamp",
                    "checkout_timestamp": "source.checkout_timestamp",
                    "annual_pass_flag": "source.annual_pass_flag",
                    "product_name": "source.product_name",
                    "valid_sk": "source.valid_sk",
                    "source_name": "source.source_name",
                    "etl_updated_datetime": "source.etl_updated_datetime",
                    "others": "source.others"
                }
            ) \
            .whenNotMatchedInsertAll() \
            .execute()

            result_row = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count = result_row["num_updated_rows"]
        else:
            print("Full Load")

            if not table_exists:
                try:
                    (spark.createDataFrame([], df_location_inteligence_facepass.schema)
                        .write.format("delta")
                        .saveAsTable(target_table))
                except Exception:
                    print("Table was created concurrently -- appending instead.")
                    pass

            # Single data write — always exactly one append, no partitionBy needed
            (df_location_inteligence_facepass
                .write.format("delta")
                .mode("append")
                .saveAsTable(target_table))
        
            # Get inserted row count from Delta table history — avoids double computation
            history_metrics = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").select("operationMetrics").collect()[0][0]
            inserted_count = int(history_metrics.get("numOutputRows", 0))

        print("Location Intlligence Facepass Data Loaded")
        print(f"Rows Inserted: {inserted_count}")
        print(f"Rows Updated: {updated_count}")
        
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_location_inteligence_facepass.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,tbl readiness - update tag
readiness_table = f"transformation_{env}.plt_customer360.readiness_location_intelligence_facepass"

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
 
