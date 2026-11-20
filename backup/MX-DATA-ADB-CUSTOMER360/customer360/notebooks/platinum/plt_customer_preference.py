# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC **The `customer_preference` table serves as the consolidated platinum-level customer communication preference data product, integrating data from SCV bronze view and SCV account master. It enforces schema consistency and supports full load, incremental and date range load strategies.**
# MAGIC
# MAGIC 1. Source Tables:
# MAGIC
# MAGIC  Schema | Table | Purpose |
# MAGIC  --- | --- | --- |
# MAGIC  brz_scv | vw_dwh_mdm_f_communication_preference | Source — customer preference data |
# MAGIC  plt_customer360 | scv_account_master | SCV master — owner_scv_sk lookup |
# MAGIC
# MAGIC 2. Target Platinum table: **transformation_dev.plt_customer360.customer_preference**
# MAGIC
# MAGIC 3. Load Strategy: **Full Load (Run 1: TRUNCATE + INSERT) | Incremental (Run 2: MERGE upsert on email + f_comm_pref_key) | Date Range (start_date and end_date provided)**
# MAGIC
# MAGIC 4. NOT NULL filter: **EMAIL IS NOT NULL AND F_Comm_Pref_Key IS NOT NULL (applied after email_standardization)**
# MAGIC
# MAGIC 5. Watermark column: **modified_date (source Modified_Date) **
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

# DBTITLE 1,include base functions
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

# DBTITLE 1,import dependencies
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, to_timestamp
from zoneinfo import ZoneInfo
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,initialize source tables
# source schema
src_db  = f"transformation_{env}.brz_scv"
tgt_db  = f"transformation_{env}.plt_customer360"

# source tables
src_pref = f"{src_db}.vw_dwh_mdm_f_communication_preference"
src_scv  = f"{tgt_db}.scv_account_master"

# target table
target_table = f"transformation_{env}.plt_customer360.customer_preference"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table : {src_pref}")
print(f"Source table : {src_scv}")
print(f"Target table : {target_table}")
#print(f"UAE timestamp      : {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,filter condition
#--------------------build filter condition before raw scan--------------------
#--------------------scan SQL — only filtered records go through transformation--------------------
if start_date.strip() and end_date.strip():
    filter_condition = f"and DATE(Modified_Date) between '{start_date}' and '{end_date}'"
    etl_watermark    = None
elif spark.catalog.tableExists(target_table):
    watermark_row    = spark.sql(f"""
        SELECT MAX(comm_pref_updated_date) as source_watermark,
               MAX(etl_updated_datetime)   as etl_watermark
        FROM {target_table}
    """).collect()[0]
    source_watermark = watermark_row["source_watermark"]
    etl_watermark    = watermark_row["etl_watermark"]
    filter_condition = f"and Modified_Date > '{source_watermark}'" if source_watermark is not None else ''
else:
    filter_condition = ''
    etl_watermark    = None

# COMMAND ----------

# DBTITLE 1,customer preference raw scan
spark.sql(f"""
            --------------------scan source table once--------------------
            --------------------filter_condition applied — only filtered records go through transformation--------------------
            select * from (
                select
                    {email_standardization('email')}               as EMAIL
                    ,Facility_Code
                    ,Channel_Type
                    ,Channel_Name
                    ,Opt_In_Ind
                    ,Created_Date
                    ,F_Comm_Pref_Key
                    ,Modified_Date
                from {src_pref}
            )
            where EMAIL           is not null
              and F_Comm_Pref_Key is not null
              and Opt_In_Ind in ('Y','N')
              {filter_condition}
""").createOrReplaceTempView("customer_preference_filtered")

# COMMAND ----------

# DBTITLE 1,Transformation and cleaning
df_customer_preference = spark.sql(f"""
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
            --------------------CTE: latest row per EMAIL + F_Comm_Pref_Key--------------------
            ,deduped as (
                select *
                       ,ROW_NUMBER() OVER (
                            PARTITION BY EMAIL, F_Comm_Pref_Key
                            ORDER BY Modified_Date DESC
                        ) as rn
                from customer_preference_filtered
            )
            --------------------customer preference main query--------------------
            select
                 map('scv_sk',cast(scv.scv_account_sk as string),
                   'omni_sk',NULL,
                   'email_id', guest.EMAIL
                   )                                                as entity_identifiers
                ,scv.scv_account_sk                                 as owner_scv_sk
                ,cast(null as string)                               as owner_omni_sk
                ,guest.EMAIL                                        as email
                ,guest.Facility_Code                                as preference_facility_code
                ,guest.Channel_Type                                 as channel_type
                ,guest.Channel_Name                                 as channel_name
                ,guest.Opt_In_Ind                                   as opt_in_ind
                ,guest.F_Comm_Pref_Key                              as f_comm_pref_key
                ,guest.Created_Date                                 as comm_pref_created_date
                ,ifnull(guest.Modified_Date,guest.Created_Date )    as comm_pref_updated_date
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
df_customer_preference = apply_schema_casting(df_customer_preference, customer_preference_schema)

# COMMAND ----------

# DBTITLE 1,write to UC
# Build DDL string from schema dict
customer_preference_ddl = ",\n".join(
    f"{col_name} {dtype}" for col_name, dtype in customer_preference_schema.items()
)

try:
    ddl_struct = StructType.fromDDL(customer_preference_ddl)
    df_customer_preference = df_customer_preference \
        .withColumn("source_name",          lit("SCV")) \
        .withColumn("etl_created_datetime", to_timestamp(lit(current_uae_timestamp))) \
        .withColumn("etl_updated_datetime", to_timestamp(lit(current_uae_timestamp)))

    if compare_schema_lenient(df_customer_preference.schema, ddl_struct):
        print("Schema matches!")

        #--------------------load mode--------------------
        inserted_count = 0
        updated_count  = 0
        table_exists   = spark.catalog.tableExists(target_table)
        watermark      = etl_watermark  
        use_date_range = bool(start_date.strip() and end_date.strip())
        is_full_load   = watermark is None and not use_date_range

        if use_date_range:
            print(f"Load mode  : DATE RANGE ({start_date} to {end_date})")
        elif is_full_load:
            print(f"Load mode  : FULL LOAD")
        else:
            print(f"Load mode  : INCREMENTAL (watermark: {watermark})")

        #--------------------update_set built once — reused for date range and incremental--------------------
        #--------------------excludes merge keys + etl_created_datetime — preserved on match--------------------
        exclude_cols = {"email", "f_comm_pref_key", "etl_created_datetime"}
        update_set   = {col: f"src.{col}" for col in customer_preference_schema.keys() if col not in exclude_cols}

        if is_full_load:
            #--------------------full load — TRUNCATE + INSERT--------------------
            if table_exists:
                spark.sql(f"TRUNCATE TABLE {target_table}")
            (df_customer_preference.write.format("delta").mode("append")
                .saveAsTable(target_table))
            inserted_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
            updated_count  = 0

        else:
            #--------------------incremental / date range — MERGE on email + f_comm_pref_key--------------------
            #--------------------selective update — etl_created_datetime preserved on match--------------------
            delta_tgt = DeltaTable.forName(spark, target_table)
            df_result = (delta_tgt.alias("tgt")
                .merge(df_customer_preference.alias("src"),
                       "tgt.email = src.email AND tgt.f_comm_pref_key = src.f_comm_pref_key")
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsertAll()
                .execute())
            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

        print(f"Customer Preference Table Loaded...")
        print(f"Rows Inserted : {inserted_count}")
        print(f"Rows Updated  : {updated_count}")
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_customer_preference.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,update_latest_tag
#view creation
catalog     = f"transformation_{env}"
schema      = "plt_customer360"
object_name = "vw_customer_preference"
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

# DBTITLE 1,trigger workflow
trigger_databricks_job_by_name('workflow_orchestration')
