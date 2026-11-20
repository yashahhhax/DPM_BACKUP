# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC **The `scv_omni_link` target table provides a unified view of account data by consolidating information from multiple gold source tables. This enables comprehensive platinum-level analytics, supports schema enforcement, and is loaded using an overwrite strategy for data consistency.**
# MAGIC 1. Source Gold Table: 
# MAGIC | Schema | Table |
# MAGIC | --- | --- |
# MAGIC |gld_drr |dwh_lkp_vw_account |
# MAGIC |gld_drr | tbvw_sales_transactions|
# MAGIC |slv_facepassreporting | slv_lkp_frictionless_data_tickets_snapshot|
# MAGIC |slv_facepassreporting | slv_lkp_frictionless_data_account_snapshot|
# MAGIC |plt_customer360| scv_account_master|
# MAGIC
# MAGIC 2. Target Platinum table: **scv_omni_link**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 
# MAGIC
# MAGIC 4. Load Strategy: **Full Load**
# MAGIC

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
# dbutils.widgets.text("env","dev")
# env = dbutils.widgets.get("env")
# dbutils.widgets.text("current_job_run_id", "")
# current_job_run_id = dbutils.widgets.get("current_job_run_id")

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include base fucntions
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,get notebook parameters
try:
    notebook_vars = [
        'env','current_job_run_id'
    ]
    env,current_job_run_id= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")


# COMMAND ----------

# DBTITLE 1,import  dependencies
from pyspark.sql.functions import col, when, current_timestamp, trim, lower, lit, regexp_replace,regexp_extract, instr, split, row_number, to_date, date_format, upper,min,max,expr,coalesce,length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime
from zoneinfo import ZoneInfo


# COMMAND ----------

# DBTITLE 1,initialize bronze and silver tables
#source schema
drr_schema = f"transformation_{env}.gld_drr"
facepass_schema = f"transformation_{env}.gld_facepassreporting"
#source table
account_table = f"{drr_schema}.dwh_lkp_vw_account"
sales_transaction_table = f"{drr_schema}.tbvw_sales_transactions"
data_ticket_table = f"transformation_{env}.slv_facepassreporting.slv_lkp_frictionless_data_tickets_snapshot"

#target schema
platinum_schema = f"transformation_{env}.plt_customer360"
#target table
target_table = f"{platinum_schema}.scv_omni_link"


#scv account master
scv_account_master = f"{platinum_schema}.scv_account_master"
current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table : {account_table}")
print(f"Source table : {sales_transaction_table}")
print(f"Source table : {data_ticket_table}")
print(f"Target table : {target_table}")
print(f"Current timestamp : {current_uae_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC Omni master Logic

# COMMAND ----------

# DBTITLE 1,omni master df
df_scv_omni_link = spark.sql(f"""
          --/// Logic 1 : get yas_id and account id through friction less data and media code
          --/// Logic 2 : get yas_id and account id through account table
            WITH scv_account_base(
                SELECT distinct yas_id,scv_account_sk,active_yas_id_flag from {scv_account_master}
            ),
            --LOGIC 1
           sales_base as (
            select
            `AP Account ID` as account_id
            ,`Media ID` as media_id
            ,`Media Code` as media_code
            from {sales_transaction_table}
            where 1=1
            and `Annual Pass Flag` = 'Y'
            and `Void Type Description` = 'Active'
            qualify( row_number() over (partition by media_code order by `As of Date` desc)) = 1
          )
          ,fp_base as (
            select
            frictionless_data_tickets.mediaidentifier
            ,frictionless_data_tickets.mypassid
            ,frictionless_data_tickets.updated
            from {data_ticket_table} frictionless_data_tickets
            qualify( row_number() over (partition by frictionless_data_tickets.mediaidentifier order by frictionless_data_tickets.updated desc,frictionless_data_tickets.snapdate desc)) = 1
          ),union_data as(

            select
            distinct
            {yas_id_standardization("fp_base.mypassid")} as yas_id,
            sales_base.account_id as account_id,
            fp_base.updated as last_modified_timestamp,
            'facepass' as SOURCE
            from sales_base
            inner join fp_base
            on fp_base.mediaidentifier::string = sales_base.media_code::string
            -- Union to avoid duplicates
            UNION 
            --LOGIC 2
            select distinct
            {yas_id_standardization("yasid")} as yas_id,
            accountid as account_id,
            UPDDATETIME as last_modified_timestamp,
            'dwh_lkp_vw_account' as SOURCE
            from {account_table}
            where 1=1
            and parentaccountid is null
            and nullif(yasid,'') is not null
          ),link_table as(
          --chek latest yas_id from scv account master
          select
          SHA2(cast(ud.account_id as string),256) as omni_account_sk,
          scv_account_sk,
          last_modified_timestamp,
          ud.source
          from union_data ud
          inner join scv_account_base scv on scv.yas_id = ud.yas_id 
          and scv.active_yas_id_flag = 'Y'
          -- qualify(row_number() over (partition by omni_account_sk,scv_account_sk order by last_modified_timestamp desc)) = 1
          --//inner join will eliminate extra yas_id from union_data
          )
          select 
          {hash_column("concat(omni_account_sk,scv_account_sk,source)")} as scv_omni_sk,
          scv_account_sk,
          omni_account_sk,
          source as source_name,
          last_modified_timestamp as source_last_modified_datetime,
          row_number() over(partition by scv_account_sk order by last_modified_timestamp desc ) as scv_rank,
          row_number() over(partition by omni_account_sk order by last_modified_timestamp desc ) as omni_rank,
          row_number() over(partition by scv_account_sk,source order by last_modified_timestamp desc ) as scv_source_rank,
          row_number() over(partition by omni_account_sk,source order by last_modified_timestamp desc ) as omni_source_rank,
          '{current_uae_timestamp}' as etl_created_datetime,
          '{current_uae_timestamp}' as etl_updated_datetime
          from link_table

""")

# df_omni_scv_link.createOrReplaceTempView("scv_omni_link")


# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_scv_omni_link = apply_schema_casting(df_scv_omni_link, scv_omni_link_schema)


# COMMAND ----------

# DBTITLE 1,write to UC
# Build DDL string from schema dict
scv_omni_link_dll = ",\n".join(
    f"{col} {dtype}" for col, dtype in scv_omni_link_schema.items()
)
 
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(scv_omni_link_dll)
    if compare_schema_lenient(df_scv_omni_link.schema, ddl_struct):
        print("Schema matches!")
        (df_scv_omni_link.write.format("delta")
                        .mode("overwrite")
                        .option("mergeSchema", "true")
                        .saveAsTable(target_table))
        row_count = spark.table(target_table).count()
        print(f"SCV Omni Link Table Loaded...")
        print(f"Total Records: {row_count}")
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_scv_omni_link.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,update_latest_tag
# add last_updated tag with current timestamp to identify table last refreshed datetime.
add_last_updated_tag(target_table)

# COMMAND ----------

# DBTITLE 1,table log
# Prepare platinum table statistics dictionary for logging

plt_stats = {
    "gld_table": target_table,
    "dim_new_records_count": row_count,
    "dim_updated_records_count": 0,
    "fact_inserted_records_count": 0,
    "fact_updated_records_count": 0
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)
 

# COMMAND ----------

# DBTITLE 1,triger workflow
trigger_databricks_job_by_name('workflow_orchestration')
