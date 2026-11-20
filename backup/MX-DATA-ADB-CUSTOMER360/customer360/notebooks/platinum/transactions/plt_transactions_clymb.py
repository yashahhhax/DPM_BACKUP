# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC This notebook processes sales and attendance data, enforces schema consistency, and incrementally loads processed results into the `transactions` table for analytics.
# MAGIC 1. Source Gold Table: 
# MAGIC | Schema | Table |
# MAGIC | --- | --- |
# MAGIC |gld_travelbox |dwh_vw_sales |
# MAGIC | gld_drr |tbvw_sales_transactions|
# MAGIC
# MAGIC 2. Target Platinum table: **transactions**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 
# MAGIC
# MAGIC 4. Load Strategy: **Incremental Load (18 months)**
# MAGIC

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
# dbutils.widgets.text("env","dev")
# env = dbutils.widgets.get("env")

# dbutils.widgets.text('start_date','')
# start_date = dbutils.widgets.get('start_date')
# dbutils.widgets.text('end_date','')
# end_date = dbutils.widgets.get('end_date')

# dbutils.widgets.text('current_job_run_id','')
# dbutils.widgets.text('buffer_days','')

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include base fucntions
# MAGIC %run ../../../shared_code/common_functions/base_functions

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
#source table
sales_transactions = f"transformation_{env}.gld_drr.tbvw_sales_transactions"
company = f"transformation_{env}.gld_shared.dwh_dim_company"
data_ticket_table = f"transformation_{env}.gld_facepassreporting.gld_frictionless_data_tickets"
data_account_table = f"transformation_{env}.gld_facepassreporting.gld_frictionless_data_accounts"

#target table
target_table = f"transformation_{env}.plt_customer360.transactions"
#master tables
scv_account_master = f"transformation_{env}.plt_customer360.scv_account_master"
omni_account_master = f"transformation_{env}.plt_customer360.omni_account_master"


current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")



print(f"Source table: {sales_transactions}")
print(f"Source Table: {data_ticket_table}")
print(f"Source Table: {data_account_table}")
print(f"Target table: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,filter condition

if (not spark.catalog.tableExists(target_table)) or (spark.sql(f"SELECT 1 FROM {target_table} WHERE SALE_SOURCE_SYSTEM = 'TUNNEL' LIMIT 1").count() == 0):
    print("Table not exists or TUNNEL data not present,Full Load")
    sales_transaction_filter = "1=1"

elif start_date and end_date and start_date < end_date:
    print("Table exists, Incremental Load with date range")
    sales_transaction_filter = f"cast(`Transaction Date Time` as date) BETWEEN '{start_date}' AND '{end_date}'"
else:
    print("Table exists, Incremental Load")
    #apply buffer days
    timestamp = (datetime.now(ZoneInfo("Asia/Dubai")) - timedelta(days=buffer_days)).strftime('%Y-%m-%d')
    #set attendance filter
    sales_transaction_filter = f"cast(`Transaction Date Time` as date) >= '{timestamp}'"

print(f"Sales Transaction filter : {sales_transaction_filter}")


# COMMAND ----------

# DBTITLE 1,IG transactions
df_transactions_clymb_raw = spark.sql(f"""
  with rank as (    
    select clymb_silver.* except(clymb_silver.`Unit Price After discount`,
                              clymb_silver.`Unit Price before discount`, 
                              clymb_silver.`Tender Amount (in AED)`, 
                              clymb_silver.`Tax Amount`,
                              clymb_silver.`Quantity`,
                              clymb_silver.`Net Sale Amount`,
                              clymb_silver.`Net Sale Amount (-Tax)`,
                              clymb_silver.`Gross Sale Amount`, 
                              clymb_silver.`Discount Quantity`,
                              clymb_silver.`Discount Amount`, 
                              clymb_silver.`Discount Amount(incl VAT)`)
      ,clymb_silver.`Email` AS EMAIL_ADDRESS
      ,row_number() over(partition by clymb_silver.`Ticket ID` order by clymb_silver.`Transaction Date Time` desc) as row_n
      ,sum(clymb_silver.`Unit Price After discount`)     over(partition by clymb_silver.`Ticket ID`) as `Unit Price After discount` 
      ,sum(clymb_silver.`Unit Price before discount`)    over(partition by clymb_silver.`Ticket ID`) as `Unit Price before discount`
      ,sum(clymb_silver.`Tender Amount (in AED)`)        over(partition by clymb_silver.`Ticket ID`) as `Tender Amount (in AED)`
      ,sum(clymb_silver.`Tax Amount`)                    over(partition by clymb_silver.`Ticket ID`) as `Tax Amount` 
      ,sum(clymb_silver.`Quantity`)                      over(partition by clymb_silver.`Ticket ID`) as `Quantity` 
      ,sum(clymb_silver.`Net Sale Amount`)               over(partition by clymb_silver.`Ticket ID`) as `Net Sale Amount` 
      ,sum(clymb_silver.`Net Sale Amount (-Tax)`)        over(partition by clymb_silver.`Ticket ID`) as `Net Sale Amount (-Tax)` 
      ,sum(clymb_silver.`Gross Sale Amount`)             over(partition by clymb_silver.`Ticket ID`) as `Gross Sale Amount` 
      ,sum(clymb_silver.`Discount Quantity`)             over(partition by clymb_silver.`Ticket ID`) as `Discount Quantity` 
      ,sum(clymb_silver.`Discount Amount`)               over(partition by clymb_silver.`Ticket ID`) as `Discount Amount`
      ,sum(clymb_silver.`Discount Amount(incl VAT)`)     over(partition by clymb_silver.`Ticket ID`) as `Discount Amount(incl VAT)`

  from {sales_transactions} clymb_silver
  where clymb_silver.`Source Name` = 'TUNNEL' 
  AND {sales_transaction_filter}
  )
  select * except(row_n,
                  `YAS ID`,`AP MYPASS ID`, 
                   `Unit Price After discount`,
                   `Unit Price before discount`, 
                   `Tender Amount (in AED)`,
                   `Tax Amount`, 
                   `Quantity`,
                   `Net Sale Amount`, 
                   `Net Sale Amount (-Tax)`, 
                   `Gross Sale Amount`, 
                   `Discount Quantity`,
                   `Discount Amount`),
     {yas_id_standardization("`YAS ID`")} AS `YAS ID`, 
    NULL AS `AP MYPASS ID`, -- NULL in CLYMB
     (case when `Unit Price After discount` < 0 then 0 else `Unit Price After discount` end ) as `Unit Price After discount`,
     (case when `Unit Price before discount` < 0 then 0 else `Unit Price before discount` end ) as `Unit Price before discount`,
     (case when `Tender Amount (in AED)` < 0 then 0 else `Tender Amount (in AED)` end ) as `Tender Amount (in AED)`,
     (case when `Tax Amount` < 0 then 0 else `Tax Amount` end ) as `Tax Amount`,
     (case when `Quantity` < 0 then 0 else `Quantity` end ) as `Quantity`,
     (case when `Net Sale Amount` < 0 then 0 else `Net Sale Amount` end ) as `Net Sale Amount`,
     (case when `Net Sale Amount (-Tax)` < 0 then 0 else `Net Sale Amount (-Tax)` end ) as `Net Sale Amount (-Tax)`,
     (case when `Gross Sale Amount` < 0 then 0 else `Gross Sale Amount` end ) as `Gross Sale Amount`, 
     (case when `Discount Quantity` < 0 then 0 else `Discount Quantity` end ) as `Discount Quantity`,
     (case when `Discount Amount` < 0 then 0 else `Discount Amount` end ) as `Discount Amount`
  from rank 
  where row_n = 1 and `Ticket ID` is not null
""")

df_transactions_clymb_raw.createOrReplaceTempView("transactions_clymb_raw")

# COMMAND ----------

# DBTITLE 1,Transformation
df_transactions_clymb = spark.sql(f"""
WITH scv_parent as (
    SELECT 
    email_id,scv_account_sk
    FROM {scv_account_master}
    where source_name = 't_mdm_guest'
    QUALIFY row_number() over(partition by email_id order by modified_date desc) = 1
)
SELECT
    coalesce(scv.scv_account_sk,scv2.scv_account_sk) AS purchaser_scv_sk,
    purchaser_scv_sk AS owner_scv_sk,
    NULL AS purchaser_omni_sk, --NULL in CLYMB
    NULL  AS owner_omni_sk, -- NULL in CLYMB
    NULL AS FL_SCV_SK, -- NULL in CLYMB
    
    map('scv_sk', cast(purchaser_scv_sk as string),
     'omni_sk', NULL,
     'email_id', cast(coalesce(clymb.EMAIL_ADDRESS,scv.email_id) as string)
     ) as entity_identifiers,


    `Transaction ID` as Transaction_id,
    `OMNI Account ID` as omni_account_id,
    EMAIL_ADDRESS,
    `YAS ID` as yas_id,

    NULL as B2B_ACCOUNT_ID,
    (case 
        when scv.staff_flag = 'Y'  or omni_acc.staff_flag = 'Y' or clymb.`Market Segment Group 1` = 'Miral Colleagues' then 'Miral Colleagues'
        WHEN clymb.`Market Segment Group 1` IN ('T&T', 'Corporate and Government', 'Partner Hotels', 'School Groups', 'Sponsors and Partners') THEN 'B2B'
        WHEN clymb.`Market Segment Group 1` IN ('Experience Hub') AND length(clymb.`B2B Account ID`) > 1 THEN 'B2B'
        WHEN clymb.`Market Segment Group 1` IN ('General Public') THEN 'B2C'
        WHEN clymb.`Market Segment Group 1` = 'Experience Hub' AND clymb.`B2B Account ID` is null THEN 'B2B&B2C'
    END) as CUSTOMER_TYPE,
    clymb.`Account Manager Name` as ACCOUNT_MANAGER_NAME,
    -- SALE RELATED
    clymb.`Sale ID/Check Number` as SALE_ID,
    clymb.`Transaction Date Time` as SALE_TIMESTAMP,
    clymb.`Transaction Date Time` as TRANSACTION_TIMESTAMP,

    upper("TUNNEL") as SALE_SOURCE_SYSTEM,
    -- TICKET RELATED
    (case when length(clymb.`Ticket ID`) > 0 then clymb.`Ticket ID` else NULL end) as TICKET_ID,
    clymb.`Void Type Description` as TICKET_STATUS,
    'N' as ANNUAL_PASS,  --'N' in CLYMB
    'N' as MULTI_PARK_PASS, --'N' in CLYMB
    -- NULL as YAS_PARK_PASS,
    (case when lower(clymb.`Ticket Tier`) != "unspecified" then clymb.`Ticket Tier` else null end) as TICKET_TIER,
    (case when lower(clymb.`Ticket Age`) != "unspecified" then clymb.`Ticket Age` else null end) as TICKET_AGE_CATEGORY,
    clymb.`Ticket Active From` as TICKET_VALID_FROM,
    clymb.`Ticket Active To` as TICKET_VALID_TO,
    (case when lower(clymb.`Used Flag`) not in ("not available", "not applicable") then clymb.`Used Flag` else null end) as TICKET_IS_USED,
    clymb.`Ticket Usage Date` as TICKET_USAGE_DATE,
    -- 3rdPARTY SALE RELATED ONLY (TRAVEL BOX)
    'PARK ONLY' as 3rdPARTY_SALE_PRODUCT_CATEGORY,
    NULL as 3rdPARTY_SALE_BOOKING_TYPE,
    NULL as 3rdPARTY_SALE_BOOKING_STATUS,
    NULL as 3rdPARTY_SALE_PAYMENT_STATUS,
    NULL as 3rdPARTY_SALE_CANCELATION_DATE,
    -- PRODUCT RELATED
    (case when length(clymb.`Product Code`) > 1 then clymb.`Product Code` else null end) as PRODUCT_ID,
    (case when length(clymb.`Product Name`) > 1 then clymb.`Product Name` else null end) as PRODUCT_NAME,
    (case when length(clymb.`Product Category Name`) > 1 then clymb.`Product Category Name` else null end) as PRODUCT_CATEGORY,
    clymb.`Product Sub Category Name` as PRODUCT_SUBCATEGORY,
    NULL as PARK_TYPE, --NULL in CLYMB
    clymb.`Quantity` as QUANTITY,
    (case when clymb.`Unit Price before discount` is not null then clymb.`Unit Price before discount` else null end) as UNIT_PRICE,
    (case when clymb.`Unit Price After discount` is not null then clymb.`Unit Price After discount` else null end) as DISCOUNTED_UNIT_PRICE,
    -- HOTEL RELATED
    NULL as HOTEL_NAME,
    NULL as HOTEL_NUMBER_ROOMS,
    NULL as HOTEL_NUMBER_NIGHTS,
    NULL as HOTEL_LENGTH_STAY,
    -- SALES CHANNEL RELATED
    (case when length(clymb.`Partner Company Account Name (OMNI)`) > 1 and lower(clymb.`Partner Company Account Name (OMNI)`) not in ("not available", "not applicable") then clymb.`Partner Company Account Name (OMNI)` else NULL end) as PARTNER_NAME,
    (case when length(clymb.`Ticket Sale Facility`) > 1 then clymb.`Ticket Sale Facility` else null end) as SELLING_ENTITY,
    (case when clymb.`WorkStation Channel` is not null then clymb.`WorkStation Channel` else NULL end) as CHANNEL,
    clymb.`WorkStation Area` as CHANNEL_AREA,
    -- Market Segment RELATED (Get more info on DMG)
    clymb.`OMNI Account Parent Category DMG Description` AS DMG_CATEGORY,
    clymb.`OMNI Account Category DMG Name` AS DMG_NAME,
    NULL AS ANNUAL_PASS_DMG, --NULL in CLYMB
    (case when length(clymb.`Promotion DMG`) > 1 and lower(clymb.`Promotion DMG`) not in ('na', "not available", "not applicable") then clymb.`Promotion DMG` else null end) AS PROMOTOIN_DMG,
    (case when length(clymb.`Promotion DMG Description`) > 1 and lower(clymb.`Promotion DMG Description`) not in ('na', "not available", "not applicable") then clymb.`Promotion DMG Description` else null end) AS PROMOTOIN_DMG_DESCRIPTION,
    clymb.`Market Segment Group 1` AS MARKET_SEGMENT_GROUP1,
    clymb.`Market Segment Group 2` AS MARKET_SEGMENT_GROUP2,
    NULL AS 3RDPARTY_MARKET_SEGMENT,
    NULL AS 3RDPARTY_MARKET_REGION,
    -- PROMOTION RELATED
    (case when lower(clymb.`OMNI Promotion Name`) not in ("not available", "not applicable") then clymb.`OMNI Promotion Name` else null end) as PROMOTION_NAME,
    (case when length(clymb.`Promotion Type`) > 1 then clymb.`Promotion Type` else null end) as PROMOTION_TYPE,
    (case when length(clymb.`Promotion Classification`) > 1 then clymb.`Promotion Classification` else null end) as PROMOTION_CLASS,
    clymb.`Promotion Valid From` as PROMOTION_FROM_DATE,
    clymb.`Promotion Valid TO` as PROMOTION_TO_DATE,
    (case when lower(clymb.`Complimentary Product Flag`) not in ("not available", "not applicable") then clymb.`Complimentary Product Flag` else null end) as COMPLEMENTRY_ITEM,
    -- SALE PRICES RELATED
    clymb.`Gross Sale Amount` AS GROSS_SALE_AMOUNT,
    (case when length(clymb.`Discount Amount`) > 0 then clymb.`Discount Amount` else null end) AS DISCOUNT_AMOUNT,
    clymb.`Net Sale Amount (-Tax)` AS NET_SALE_AMOUNT_NO_TAX,
    clymb.`Net Sale Amount` AS NET_SALE_AMOUNT,
    clymb.`Revenue Stream / GL Group Name` AS REVENUE_TYPE,
    clymb.`Transaction Type` as TRANSACTION_TYPE
    
FROM transactions_clymb_raw AS clymb

LEFT JOIN {scv_account_master} scv 
    ON scv.yas_id = clymb.`YAS ID` AND scv.active_yas_id_flag = 'Y'

LEFT JOIN scv_parent scv2
    ON scv2.email_id = clymb.EMAIL_ADDRESS

LEFT JOIN {omni_account_master} omni_acc 
    ON omni_acc.account_id = clymb.`OMNI Account ID`

WHERE clymb.`YAS ID` is not null 
OR clymb.`OMNI Account ID` is not null
OR clymb.EMAIL_ADDRESS is not null             -- need to check this logic
""")
#Asset Name standardization
df_transactions_clymb = map_asset_category(df_transactions_clymb,"SELLING_ENTITY")
df_transactions_clymb.createOrReplaceTempView("transactions_clymb")

# COMMAND ----------

# DBTITLE 1,final data
df_transactions_clymb = spark.sql(f"""
select 
   entity_identifiers,
   purchaser_scv_sk,
   purchaser_omni_sk,
   owner_scv_sk,
   owner_omni_sk,
   fl_scv_sk,
   b2b_account_id,
   customer_type,
   account_manager_name,
   sale_id,
   sale_timestamp,
   transaction_timestamp,
   sale_source_system,
   ticket_id,
   ticket_status,
   annual_pass,
   multi_park_pass,
   -- yas_park_pass,
   ticket_tier,
   ticket_age_category,
   ticket_valid_from,
   ticket_valid_to,
   ticket_is_used,
   ticket_usage_date,
   3rdparty_sale_product_category,
   3rdparty_sale_booking_type,
   3rdparty_sale_booking_status,
   3rdparty_sale_payment_status,
   3rdparty_sale_cancelation_date,
   product_id,
   product_name,
   case
        when trim(lower(product_category)) like 'f%b%retail%' then 'F&B Retail'
        when trim(lower(product_category)) like 'hotel%' then 'Hotel'
        when trim(lower(product_category)) = 'unknown' then NULL
        else initcap(trim(product_category))
   end as product_category,
   product_subcategory,
   park_type,
   quantity,
   unit_price,
   discounted_unit_price,
   hotel_name,
   hotel_number_rooms,
   hotel_number_nights,
   hotel_length_stay,
   partner_name,
   selling_entity,
   channel,
   channel_area,
   dmg_category,
   dmg_name,
   annual_pass_dmg,
   promotoin_dmg,
   promotoin_dmg_description,
   market_segment_group1,
   market_segment_group2,
   3rdparty_market_segment,
   3rdparty_market_region,
   promotion_name,
   promotion_type,
   promotion_class,
   promotion_from_date,
   promotion_to_date,
   complementry_item,
   gross_sale_amount,
   discount_amount,
   net_sale_amount_no_tax,
   net_sale_amount,
   revenue_type,
   transaction_type,
   case when purchaser_scv_sk is null and
            purchaser_omni_sk is null and
            owner_scv_sk is null and
            owner_omni_sk is null then 'N'
            else 'Y' end as valid_sk,  -- all sk are null then N else Y
   sale_source_system AS source_name,
   '{current_uae_timestamp}' as etl_created_datetime,
   '{current_uae_timestamp}' as etl_updated_datetime,
   map(
      'transaction_id',cast(Transaction_id as string),
      'yas_id',cast(yas_id as string),
      'omni_account_id',cast(omni_account_id as string),
      'email_id',cast(email_address as string)

   ) as others

  from transactions_clymb 
  """)

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_transactions_clymb = apply_schema_casting(df_transactions_clymb,transactions_schema )

# COMMAND ----------

# DBTITLE 1,write to UC
# Build DDL string from schema dict
transactions_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in transactions_schema.items()
)
 
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(transactions_ddl)
    if compare_schema_lenient(df_transactions_clymb.schema, ddl_struct):
        print("Schema matches!")

        deleted_count = 0
        inserted_count = 0

        # Check table state for CLYMB data
        table_exists = spark.catalog.tableExists(target_table)
        has_clymb_data = table_exists and spark.sql(f"SELECT 1 FROM {target_table} WHERE SALE_SOURCE_SYSTEM = 'TUNNEL' LIMIT 1").count() > 0

        # Load strategy: delete old CLYMB data for incremental
        if has_clymb_data:
            if start_date and end_date and start_date < end_date:
                delete_result = spark.sql(f"""
                                          DELETE FROM {target_table} 
                                          WHERE cast(SALE_TIMESTAMP as date) BETWEEN '{start_date}' AND '{end_date}' 
                                          AND SALE_SOURCE_SYSTEM = 'TUNNEL'
                                          """)
                deleted_count = delete_result.collect()[0]['num_affected_rows']
            else:
                delete_result = spark.sql(f"""
                                          DELETE FROM {target_table} 
                                          WHERE cast(SALE_TIMESTAMP as date) >= '{timestamp}' 
                                          AND SALE_SOURCE_SYSTEM = 'TUNNEL'
                                          """)
                deleted_count = delete_result.collect()[0]['num_affected_rows']
            print("Incremental Load")
        else:
            print("Full Load")

        # Ensure table exists with partitioning — zero-cost (empty DataFrame, schema only)
        if not table_exists:
            try:
                (spark.createDataFrame([], df_transactions_clymb.schema)
                    .write.format("delta")
                    .saveAsTable(target_table))
            except Exception:
                print("Table was created concurrently -- appending instead.")
                pass  # Table was created concurrently — safe to continue

        # Single data write — always exactly one append
        (df_transactions_clymb
            .write.format("delta")
            .mode("append")
            .saveAsTable(target_table))

        
        # Get inserted row count from Delta table history — avoids double computation
        history_metrics = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").select("operationMetrics").collect()[0][0]
        inserted_count = int(history_metrics.get("numOutputRows", 0))

        print("Transactions CLYMB Data Loaded")
        print(f"Rows Deleted: {deleted_count}")
        print(f"Rows Inserted: {inserted_count}")
        
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_transactions_clymb.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,tbl readiness - update tag
readiness_table = f"transformation_{env}.plt_customer360.readiness_transactions_clymb"

df = spark.createDataFrame([(spark.sql("SELECT from_utc_timestamp(current_timestamp(), 'Asia/Dubai')").collect()[0][0],)],["last_updated_timestamp"])
display(df)

df.write.mode("append").saveAsTable(readiness_table)
add_last_updated_tag(readiness_table)

# COMMAND ----------

# DBTITLE 1,trigger orchestration
trigger_databricks_job_by_name('workflow_orchestration')

# COMMAND ----------

# Prepare platinum table statistics dictionary for logging

plt_stats = {
    "gld_table": target_table,
    "dim_new_records_count": 0,
    "dim_updated_records_count": 0,
    "fact_inserted_records_count": inserted_count,
    "fact_updated_records_count": 0
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)
 
