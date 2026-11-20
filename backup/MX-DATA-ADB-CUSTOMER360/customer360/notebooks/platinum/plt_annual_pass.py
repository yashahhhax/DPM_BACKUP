# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## This Notebook Use for Platinum Annual pass load where source data getting from gold drr schema.

# COMMAND ----------

# MAGIC  %md
# MAGIC ##  Table Summary
# MAGIC
# MAGIC |Source Tables|Use|
# MAGIC |-------------|----|
# MAGIC |gld_drr.vw_dwh_fact_annual_pass|annual_pass purchase snapshot data|
# MAGIC |plt_customer360.scv_account_master|scv(single customer view) details|
# MAGIC  
# MAGIC |Target Table|
# MAGIC |------------|
# MAGIC |plt_customer360.annual_pass|
# MAGIC
# MAGIC ### Schema enforcement implemented
# MAGIC ### Load Strategy: Full load (latest snapshot data)

# COMMAND ----------

# DBTITLE 1,schema_config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,base_functions
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,view_functions
# MAGIC %run ../../config/view_queries

# COMMAND ----------

# DBTITLE 1,get notebook params
try:
    notebook_vars = [
        'env','as_of_date','current_job_run_id'
    ]
    env,as_of_date,current_job_run_id = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,import dependencies
from pyspark.sql.functions import col, when, current_timestamp, trim, lower, lit, regexp_replace,regexp_extract, instr, split, row_number, to_date, date_format, upper,min,max,expr,coalesce,length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime
from zoneinfo import ZoneInfo

# COMMAND ----------

# DBTITLE 1,initialize source tables
#source schema
drr_schema = f"transformation_{env}.gld_drr"
platinum_schema = f"transformation_{env}.plt_customer360"

#source table
fact_annual_pass_table = f"{drr_schema}.vw_dwh_fact_annual_pass"
scv_master_table = f"{platinum_schema}.scv_account_master"
omni_master_table = f"{platinum_schema}.omni_account_master"

# Target table name
target_table = f"transformation_{env}.plt_customer360.annual_pass"

print(f"fact_annual_pass_table: {fact_annual_pass_table}")
print(f"scv_master_table: {scv_master_table}")
print(f"omni_master_table: {omni_master_table}")
print(f"target table: {target_table}")


# COMMAND ----------

if as_of_date.strip():
    filter_condition = f"`As of Date` = '{as_of_date}'"
else:
    max_date = spark.sql(f"""select max(`As of Date`) from {fact_annual_pass_table}""").collect()[0][0]
    filter_condition = f"`As of Date` = '{max_date}'"
print(f"filter_condition: {filter_condition}")

# COMMAND ----------

# DBTITLE 1,table Load
# current timestamp
current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

df_annual_pass = spark.sql(f"""
        with fact_annual_pass_cte as (
            select 
            ap.`OMNI Account ID` as omni_account_id,
            {yas_id_standardization('ap.`Omni MYPASS ID`')} as omni_mypass_id, -- hash
            ap.`AP Account` as ap_account,
            {yas_id_standardization('ap.`AP MYPASS ID`')} as ap_mypass_id,  -- hash
            ap.`As of Date` as as_of_date,
            ap.Status as status,
            ap.`Ticket Sale Facility Name` as ticket_sale_facility_name,
            ap.`Ticket Sale Facility Code` as ticket_sale_facility_code,
            ap.`Ticket ID` as ticket_id, --dedup col
            ap.`Media ID` as media_id,
            ap.`Media Code` as media_code,
            ap.`Product Code` as product_code,
            ap.`Product Name` as product_name,
            ap.`Ticket Code` as ticket_code,
            ap.`Ticket Description` as ticket_description,
            ap.`Ticket Age` as ticket_age,
            ap.`Ticket Tier` as ticket_tier,
            ap.`Rack Rate` as rack_rate,
            ap.Park as park,
            ap.Site as site,
            ap.Channel as channel,
            ap.`Market Segment Rule 1` as market_segment_rule_1,
            ap.`Market Segment Group 1` as market_segment_group_1,
            ap.`Market Segment Rule 2` as market_segment_rule_2,
            ap.`Market Segment Group 2` as market_segment_group_2,
            ap.`OMNI Promotion Name` as omni_promotion_name,
            ap.`OMNI Promotion Code` as omni_promotion_code,
            ap.`Promotion DMG Description` as promotion_dmg_description,
            ap.`Promotion Flag` as promotion_flag,
            ap.`Promotion Valid From` as promotion_valid_from,
            ap.`Promotion Valid To` as promotion_valid_to,
            ap.`Void Type` as void_type,
            ap.`Void Type Description` as void_type_description,
            ap.`Workstation ID` as workstation_id,
            ap.`WorkStation Area` as workStation_area,
            ap.`Upgrade/Downgrade` as upgrade_downgrade,
            ap.`Upgrade/Downgrade Date` as upgrade_downgrade_date,
            ap.`Acquisition/Re-Acquisition` as acquisition_reacquisition,
            ap.`Ticket Active From` as ticket_active_from,
            ap.`Ticket Active To` as ticket_active_to,
            ap.`Sale date/time` as sale_date_time,
            ap.`Voucher Name` as voucher_name,
            ap.`Unit Price Before Discount` as unit_price_before_discount,
            ap.`Unit Price After Discount` as unit_price_after_discount,
            ap.Quantity as quantity,
            ap.`Gross Sale Amount` as gross_sale_amount,
            ap.`Discount Amount` as discount_amount,
            ap.`Tax Amount` as tax_amount,
            ap.`Net Sale Amount` as net_sale_amount,
            ap.`Net Sale Amount (-Tax)` as net_sale_amount_wo_tax,
            ap.UPDATED_ON
            from {fact_annual_pass_table} ap
            where 1=1
            and {filter_condition} 
            and (ap.`Ap Account` is not null and ap.`ticket id` is not null)
            qualify (row_number() over (partition by `Ap Account`,`ticket_id` order by `UPDATED_ON` desc,Sale_date_time desc)) = 1
        )
            select 
             map('scv_sk',cast(sm2.scv_account_sk as string),
                'omni_sk',cast(om2.omni_account_sk as string),
                'email_id',cast(coalesce(om2.email_id, sm2.email_id) as string)
                ) as entity_identifiers,
            sm1.scv_account_sk as purchaser_scv_sk,
            om1.omni_account_sk as purchaser_omni_sk,
            sm2.scv_account_sk as owner_scv_sk,
            om2.omni_account_sk as owner_omni_sk, --from omni
            cap.as_of_date,
            cap.status,
            cap.ticket_sale_facility_name,
            cap.ticket_sale_facility_code,
            cap.ticket_id,
            cap.media_id,
            cap.media_code,
            cap.product_code, 
            cap.product_name,
            cap.ticket_code,
            cap.ticket_description,
            cap.ticket_age,
            cap.ticket_tier,
            cap.rack_rate,
            cap.park,
            cap.site,
            cap.channel,
            cap.market_segment_rule_1,
            cap.market_segment_group_1,
            cap.market_segment_rule_2,
            cap.market_segment_group_2,
            cap.omni_promotion_name,
            cap.omni_promotion_code,
            cap.promotion_dmg_description,
            cap.promotion_flag,
            cap.promotion_valid_from,
            cap.promotion_valid_to,
            cap.void_type,
            cap.void_type_description,
            cap.workstation_id,
            cap.workstation_area,
            cap.upgrade_downgrade,
            cap.upgrade_downgrade_date,
            cap.acquisition_reacquisition,
            cap.ticket_active_from,
            cap.ticket_active_to,
            cap.sale_date_time,
            cap.voucher_name,
            cap.unit_price_before_discount,
            cap.unit_price_after_discount,
            cap.quantity,
            cap.gross_sale_amount,
            cap.discount_amount,
            cap.tax_amount,
            cap.net_sale_amount,
            cap.net_sale_amount_wo_tax,
            --add others map col
            map('omni_mypass_id',cast(cap.Omni_MYPASS_ID as string),
                'ap_mypass_id',cast(cap.AP_MYPASS_ID as string),
                'omni_account_id',cast(cap.Omni_Account_ID as string),
                'ap_account',cast(cap.AP_Account as string)
                ) as others,
            'omni_fact_ap' as source_name,
            '{current_uae_timestamp}' AS etl_created_datetime,
            '{current_uae_timestamp}' as etl_updated_datetime
            from fact_annual_pass_cte as cap
            left join {scv_master_table} as sm1 
            on cap.`Omni_MYPASS_ID` = sm1.yas_id and sm1.active_yas_id_flag = 'Y'
            left join {scv_master_table} as sm2 
            on cap.`AP_MYPASS_ID` = sm2.yas_id and sm2.active_yas_id_flag = 'Y'
            left join {omni_master_table} as om1 on cap.omni_account_id = om1.account_id
            left join {omni_master_table} as om2 on cap.AP_Account = om2.account_id
            """)

# COMMAND ----------

# DBTITLE 1,schema casting
df_annual_pass = apply_schema_casting(df_annual_pass,annual_pass_schema)

# COMMAND ----------

# DBTITLE 1,writing to uc

# Build DDL string from schema dict

annual_pass_ddl = ",\n".join(
    f"`{col}` {dtype}" for col, dtype in annual_pass_schema.items()
)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(annual_pass_ddl)
    if compare_schema_lenient(df_annual_pass.schema, ddl_struct):
        print("Schema matches!")
        
        # if df_annual_pass.count() == 0:
        if df_annual_pass.isEmpty():
            raise Exception("Eaither Snapshot data for provided 'as_of_date' not available in source table or df has no data")
        else:
            (df_annual_pass.write.format("delta")
                        .mode("overwrite")
                        .saveAsTable(target_table))
        
# Extract counts
        target = DeltaTable.forName(spark,target_table)
        rows_inserted = int(target.history(1).select("operationMetrics.numOutputRows").collect()[0][0])

    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_annual_pass.schema}</pre>")
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
object_name = "vw_annual_pass"
select_query = c360_view_queries[object_name].format(catalog=catalog, schema=schema)
create_view_or_table(catalog=catalog, schema=schema, object_name=object_name, select_query=select_query)

# COMMAND ----------

# DBTITLE 1,plt_logging

# Prepare platinum table statistics dictionary for logging

plt_stats = {
    "gld_table": target_table,
    "dim_new_records_count": 0,
    "dim_updated_records_count": 0,
    "fact_inserted_records_count": rows_inserted,
    "fact_updated_records_count": 0
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)
 

# COMMAND ----------

# DBTITLE 1,job_trigger
#triger orchestration
trigger_databricks_job_by_name('workflow_orchestration')
