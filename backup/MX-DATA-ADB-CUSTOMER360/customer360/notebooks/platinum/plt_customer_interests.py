# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC **The `customer_interests` table serves as the consolidated platinum-level customer interests data product, integrating data from SCV gold tables. It enforces schema consistency and supports full load, incremental and date range load strategies.**
# MAGIC
# MAGIC 1. Source Tables:
# MAGIC
# MAGIC  Schema | Table | Purpose |
# MAGIC  --- | --- | --- |
# MAGIC  gld_scv | dwh_scv_guest | Source — customer interests data |
# MAGIC  plt_customer360 | scv_account_master | SCV master — owner_scv_sk lookup |
# MAGIC
# MAGIC 2. Target Platinum table: **transformation_dev.plt_customer360.customer_interests**
# MAGIC
# MAGIC 3. Load Strategy: **Full Load (Run 1: TRUNCATE + INSERT) | Incremental (Run 2+: MERGE on email + f_pref_key) | Date Range (start_date and end_date provided)**
# MAGIC
# MAGIC 4. NOT NULL filter: **EMAIL IS NOT NULL AND F_PREF_KEY IS NOT NULL**
# MAGIC
# MAGIC 5. Watermark column: **source_modified_on (source LAST_MODIFIED_ON)**
# MAGIC
# MAGIC 6. Schema Enforcement implemented

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
#dbutils.widgets.text("env","dev")
#dbutils.widgets.text("start_date","")
#dbutils.widgets.text("end_date","")
#dbutils.widgets.text("current_job_run_id","")
#
#env                = dbutils.widgets.get("env")
#start_date         = dbutils.widgets.get("start_date")
#end_date           = dbutils.widgets.get("end_date")
#current_job_run_id = dbutils.widgets.get("current_job_run_id")
#
#print(f"env                : {env}")
#print(f"start_date         : {start_date}")
#print(f"end_date           : {end_date}")
#print(f"current_job_run_id : {current_job_run_id}")

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include base fucntions
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,include view ddl
# MAGIC %run ../../config/view_queries

# COMMAND ----------

# DBTITLE 1,get notebook parameters
try:
    notebook_vars = [
        'env', 'start_date', 'end_date', 'current_job_run_id'
    ]
    env, start_date, end_date, current_job_run_id = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,import  dependencies
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, to_timestamp
from zoneinfo import ZoneInfo
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,initialize source tables
#source schema
src_db = f"transformation_{env}.gld_scv"
tgt_db = f"transformation_{env}.plt_customer360"
#source tables
src_guest = f"{src_db}.dwh_scv_guest"
src_scv   = f"{tgt_db}.scv_account_master"

# Target table name
target_table = f"transformation_{env}.plt_customer360.customer_interests"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table : {src_guest}")
print(f"Source table : {src_scv}")
print(f"Target table : {target_table}")
#print(f"UAE timestamp: {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,filter condition
#--------------------build filter condition before raw scan--------------------
#--------------------injected into raw scan SQL — only filtered records go through transformation--------------------
if start_date.strip() and end_date.strip():
    filter_condition = f"and DATE(LAST_MODIFIED_ON) between '{start_date}' and '{end_date}'"
elif spark.catalog.tableExists(target_table) and spark.sql(f"SELECT MAX(etl_updated_datetime) FROM {target_table}").collect()[0][0] is not None:
    filter_condition = f"and LAST_MODIFIED_ON > (select coalesce(max(source_modified_on), '1900-01-01') from {target_table})"
else:
    filter_condition = ''

# COMMAND ----------

# DBTITLE 1,customer interests raw scan
spark.sql(f"""
            --------------------scan source table once--------------------
            --------------------filter_condition applied — only filtered records go through transformation--------------------
            select
                {yas_id_standardization('YAS_ID')}              as YAS_ID
                ,{email_standardization('EMAIL')}               as EMAIL
                ,LEAD_SOURCE
                ,GUEST_PREFERENCE_FACILITY_CODE
                ,GUEST_PREFERENCE_SOURCE_SYSTEM_ID
                ,GUEST_PREFERENCE_NAME
                ,GUEST_PREFERENCE_VALUE
                ,GUEST_PREFERENCE_TYPE
                ,F_PREF_KEY
                ,F_COMM_PREF_KEY
                ,LAST_MODIFIED_ON                               as source_modified_on
            from {src_guest}
            where EMAIL      is not null
              and F_PREF_KEY is not null
              and GUEST_PREFERENCE_VALUE in ('Y','N')
              {filter_condition}
""").createOrReplaceTempView("customer_interests_filtered")

# COMMAND ----------

# DBTITLE 1,Transformation and cleaning
df_customer_interests = spark.sql(f"""
            --------------------CTE: deduplicate scv_account_master — one SK per email--------------------
            with scv_active as (
                select email_id, scv_account_sk
                from (
                    select email_id, scv_account_sk,
                           ROW_NUMBER() OVER (
                               PARTITION BY email_id
                               ORDER BY modified_date DESC
                           ) as rn
                    from   {src_scv}
                    where  source_name = 't_mdm_guest'
                ) where rn = 1
            )
            --------------------CTE: latest row per EMAIL + F_PREF_KEY--------------------
            ,deduped as (
                select *
                       ,ROW_NUMBER() OVER (
                            PARTITION BY EMAIL, F_PREF_KEY
                            ORDER BY source_modified_on DESC, F_COMM_PREF_KEY DESC
                        ) as rn
                from customer_interests_filtered
            )
            --------------------customer interests main query--------------------
            select
                 map('scv_sk',cast(scv.scv_account_sk as string),
                   'omni_sk',NULL,
                   'email_id', guest.EMAIL
                   )                                                as entity_identifiers
                ,scv.scv_account_sk                                 as owner_scv_sk
                ,cast(null as string)                               as owner_omni_sk
                ,guest.EMAIL                                        as email
                ,guest.YAS_ID                                       as yas_id
                ,guest.LEAD_SOURCE                                  as lead_source
                ,guest.GUEST_PREFERENCE_FACILITY_CODE               as guest_preference_facility_code
                ,guest.GUEST_PREFERENCE_SOURCE_SYSTEM_ID            as guest_preference_source_system_id
                ,guest.GUEST_PREFERENCE_NAME                        as guest_preference_name
                ,guest.GUEST_PREFERENCE_VALUE                       as guest_preference_value
                ,guest.GUEST_PREFERENCE_TYPE                        as guest_preference_type
                ,guest.F_PREF_KEY                                   as f_pref_key
                ,guest.source_modified_on                              as source_modified_on
            from deduped guest
            left join scv_active scv on guest.EMAIL = scv.email_id
            where guest.rn = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Schema casting

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_customer_interests = apply_schema_casting(df_customer_interests, customer_interests_schema)

# COMMAND ----------

# DBTITLE 1,write to UC
# Build DDL string from schema dict
customer_interests_ddl = ",\n".join(
    f"{col_name} {dtype}" for col_name, dtype in customer_interests_schema.items()
)

try:
    ddl_struct = StructType.fromDDL(customer_interests_ddl)
    df_customer_interests = df_customer_interests \
        .withColumn("source_name",          lit("SCV")) \
        .withColumn("etl_created_datetime", to_timestamp(lit(current_uae_timestamp))) \
        .withColumn("etl_updated_datetime", to_timestamp(lit(current_uae_timestamp)))

    if compare_schema_lenient(df_customer_interests.schema, ddl_struct):
        print("Schema matches!")

        #--------------------load mode--------------------
        inserted_count = 0
        updated_count  = 0
        table_exists = spark.catalog.tableExists(target_table)
        if table_exists:
            watermark = spark.sql(f"SELECT MAX(etl_updated_datetime) FROM {target_table}").collect()[0][0]
        else:
            watermark = None
        use_date_range = bool(start_date.strip() and end_date.strip())
        is_full_load   = watermark is None and not use_date_range

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
            (df_customer_interests.write.format("delta").mode("append")
                .saveAsTable(target_table))
            inserted_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
            updated_count  = 0

        else:
            #--------------------incremental / date range — MERGE on email + f_pref_key--------------------
            #--------------------selective update — etl_created_datetime preserved on match--------------------
            #--------------------update_set built dynamically from schema — excludes merge keys + etl_created_datetime--------------------
            exclude_cols  = {"email", "f_pref_key", "etl_created_datetime"}
            update_set    = {col: f"src.{col}" for col in customer_interests_schema.keys() if col not in exclude_cols}
            delta_tgt     = DeltaTable.forName(spark, target_table)
            df_result = (delta_tgt.alias("tgt")
                .merge(df_customer_interests.alias("src"),
                       "tgt.email = src.email AND tgt.f_pref_key = src.f_pref_key")
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsertAll()
                .execute())
            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

        print(f"Customer Interests Table Loaded...")
        print(f"Rows Inserted : {inserted_count}")
        print(f"Rows Updated  : {updated_count}")
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_customer_interests.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,update_latest_tag
#view creation
catalog     = f"transformation_{env}"
schema      = "plt_customer360"
object_name = "vw_customer_interests"
select_query = c360_view_queries[object_name].format(catalog=catalog, schema=schema)
create_view_or_table(catalog=catalog, schema=schema, object_name=object_name, select_query=select_query)

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

# DBTITLE 1,trigger work flow
trigger_databricks_job_by_name('workflow_orchestration')
