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
    buffer_days=int(buffer_days)
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
vw_accounts = f"transformation_{env}.gld_drr.dwh_lkp_vw_account"

#target table
target_table = f"transformation_{env}.plt_customer360.transactions"
#master tables
scv_account_master = f"transformation_{env}.plt_customer360.scv_account_master"
omni_account_master = f"transformation_{env}.plt_customer360.omni_account_master"
scv_omni_link = f"transformation_{env}.plt_customer360.scv_omni_link"


current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")



print(f"Source table: {sales_transactions}")
print(f"Source table: {vw_accounts}")
print(f"Target table: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,filter condition

if (not spark.catalog.tableExists(target_table)) or (spark.sql(f"SELECT 1 FROM {target_table} WHERE SALE_SOURCE_SYSTEM = 'IG' LIMIT 1").count() == 0):
    print("Table not exists or IG data not present,Full Load")
    sales_transaction_filter = "1=1"

elif start_date and end_date and start_date < end_date: 
    print("Table exists, Incremental Load with date range")
    sales_transaction_filter = f"cast(`Payment Date Time` as date) BETWEEN '{start_date}' AND '{end_date}'"
else:
    print("Table exists, Incremental Load")
    #apply buffer days
    timestamp = (datetime.now(ZoneInfo("Asia/Dubai")) - timedelta(days=buffer_days)).strftime('%Y-%m-%d')
    #set attendance filter
    sales_transaction_filter = f"cast(`Payment Date Time` as date) >= '{timestamp}'"

print(f"Sales Transaction filter : {sales_transaction_filter}")


# COMMAND ----------

# DBTITLE 1,annual pass discount codes
spark.sql(f"""
          select 
          discount_code
          from (values
            '119','125','126','128','129','130','150','151','257','258','259','260', '120', '186','275','46' ,'307' ,'276' ,'163' ,'309' ,'310' ,'274' ,'297' ,'311' ,'308' ,'6' ,'9250' ,'92217' ,'916' ,'9251' ,'248' ,'9270' ,'9226' ,'9226' ,'210' ,'250' ,'248' ,'91101' ,'235' ,'235' ,'217' ,'9162' ,'9163' ,'9161'
          ) as discount_codes(discount_code)
          
          """).createOrReplaceTempView("ap_discount_codes")

# COMMAND ----------

# DBTITLE 1,Transformation
df_transactions_ig_raw = spark.sql(f"""
WITH ig_silver AS (
    SELECT *
    FROM {sales_transactions} ig_silver
    WHERE ig_silver.`Source Name` = 'IG'
      AND {sales_transaction_filter}
),
mappable_annual_pass_accounts AS (
    SELECT 
        CAST(`AP Account ID` AS STRING) AS AP_ACCOUNT_ID,
        `Media Code` AS Media_Code
    FROM {sales_transactions}
    WHERE 1=1 
      AND LENGTH(`AP Account ID`) > 1
      AND LOWER(`AP Account ID`) NOT IN ("not applicable", "not available")
      AND `Annual Pass Flag` = 'Y'
      AND `Source Name` = 'OMNI_NEW'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY `Media Code` ORDER BY `Transaction Date Time` DESC) = 1
),
mappable_emp_accounts as (
    select 
        distinct
        accountid
        ,YASID
        ,trim(externalcode1) as employeeid
    from {vw_accounts}
    where  dmgcategoryid in (121,182,40)
        and try_cast(externalcode1 as int) is not null
        and length(externalcode1) = 5
    qualify( row_number() over (partition by trim(externalcode1) order by upddatetime desc)) = 1
),
ig_gold AS (
    SELECT
        ig.*,
        CASE 
            WHEN ig.`Memo Text` like '%-%-%-%-%' then {yas_id_standardization("ig.`Memo Text`")} 
            WHEN regexp_like(lower(ig.`OMNI Promotion Name`), r'(coll|office|miral|duty|staff)') and emp.yasid is not null THEN {yas_id_standardization("emp.yasid")}
            ELSE NULL
        END as Memo_yas_id,
        CASE 
            WHEN `IG Discount Code` in (select discount_code from ap_discount_codes) and length(ig.`Memo Text`) > 7 and try_cast(ap.AP_ACCOUNT_ID as int) is not null then try_cast(ap.AP_ACCOUNT_ID as int)
            WHEN `IG Discount Code` in (select discount_code from ap_discount_codes) and try_cast(trim(ig.`Memo Text`) as int ) between 10000 and 9999999 THEN  try_cast(trim(ig.`Memo Text`) as int )
            when regexp_like(lower(ig.`OMNI Promotion Name`), r'(coll|office|miral|duty|staff)') and emp.accountid is not null THEN emp.accountid::int
            when regexp_like(lower(ig.`OMNI Promotion Name`), r'(coll|office|miral|duty|staff)') and omni.account_id is not null THEN omni.account_id::int
            ELSE NULL 
        END  AS Memo_Account_ID
        -- ap.AP_ACCOUNT_ID as memo_AP_Account_ID
    FROM ig_silver AS ig
    LEFT JOIN mappable_annual_pass_accounts ap ON trim(ig.`Memo Text`)::string = ap.Media_Code::string 
    LEFT JOIN mappable_emp_accounts emp ON trim(ig.`Memo Text`)::string = emp.employeeid::string
    LEFT JOIN {omni_account_master} omni on omni.account_id = try_cast(trim(ig.`Memo Text`) as int ) and omni.staff_flag = 'Y'
)
SELECT
    ig.`Media Code`,
    ig.Memo_Yas_ID,
    ig.Memo_Account_ID,
    ig.`Transaction ID` as Transaction_id,
    ig.`Memo Text` as memo_text,
    
    NULL AS B2B_ACCOUNT_ID,
    NULL AS ACCOUNT_MANAGER_NAME,

    -- SALE RELATED
    CASE WHEN LENGTH(ig.`Sale ID/Check Number`) > 1 THEN ig.`Sale ID/Check Number` ELSE NULL END AS SALE_ID,
    CASE WHEN LENGTH(ig.`Payment Date Time`) > 1 THEN ig.`Payment Date Time` ELSE NULL END AS SALE_TIMESTAMP,
    NULL AS TRANSACTION_TIMESTAMP,
    "IG" AS SALE_SOURCE_SYSTEM,

    -- TICKET RELATED
    NULL AS TICKET_ID,
    NULL AS TICKET_STATUS,
    
    CASE WHEN `IG Discount Code` in (select discount_code from ap_discount_codes)  THEN 'Y' ELSE 'N' END AS ANNUAL_PASS,
    NULL AS MULTI_PARK_PASS,
    -- CASE WHEN LENGTH(ig.`Yas Park Pass`) > 1 THEN ig.`Yas Park Pass` ELSE NULL END AS YAS_PARK_PASS,
    NULL AS TICKET_TIER,
    NULL AS TICKET_AGE_CATEGORY,
    NULL AS TICKET_VALID_FROM,
    NULL AS TICKET_VALID_TO,
    NULL AS TICKET_IS_USED,
    NULL AS TICKET_USAGE_DATE,

    -- 3rdPARTY SALE RELATED ONLY (TRAVEL BOX)
    cast(NULL AS STRING) AS 3rdPARTY_SALE_PRODUCT_CATEGORY,
    cast(NULL AS STRING) AS 3rdPARTY_SALE_BOOKING_TYPE,
    cast(NULL AS STRING) AS 3rdPARTY_SALE_BOOKING_STATUS,
    cast(NULL AS STRING) AS 3rdPARTY_SALE_PAYMENT_STATUS,
    cast(NULL AS TIMESTAMP) AS 3rdPARTY_SALE_CANCELATION_DATE,

    -- PRODUCT RELATED
    CASE WHEN LENGTH(ig.`Menu Item ID`) > 1 THEN ig.`Menu Item ID` ELSE NULL END AS PRODUCT_ID,
    CASE WHEN LENGTH(ig.`Menu Item Name`) > 1 THEN ig.`Menu Item Name` ELSE NULL END AS PRODUCT_NAME,
    CASE
        WHEN LENGTH(ig.`Product Category Name`) > 1 AND ig.`Product Category Name` = "Beverage - NA" THEN "Beverage"
        WHEN LENGTH(ig.`Product Category Name`) > 1 THEN ig.`Product Category Name`
        ELSE NULL
    END AS PRODUCT_CATEGORY,
    CASE WHEN LENGTH(ig.`Product Sub Category Name`) > 1 THEN ig.`Product Sub Category Name` ELSE NULL END AS PRODUCT_SUBCATEGORY,
    NULL AS PARK_TYPE,
    ig.`Quantity` AS QUANTITY,
    CASE WHEN ig.`Unit Price before discount` IS NOT NULL THEN ig.`Unit Price before discount` ELSE NULL END AS UNIT_PRICE,
    CASE WHEN ig.`Unit Price After discount` IS NOT NULL THEN ig.`Unit Price After discount` ELSE NULL END AS DISCOUNTED_UNIT_PRICE,

    -- HOTEL RELATED
    NULL AS HOTEL_NAME,
    NULL AS HOTEL_NUMBER_ROOMS,
    NULL AS HOTEL_NUMBER_NIGHTS,
    NULL AS HOTEL_LENGTH_STAY,

    -- SALES CHANNEL RELATED
    NULL AS PARTNER_NAME,
    CASE WHEN LENGTH(ig.`Ticket Sale Facility`) > 1 THEN ig.`Ticket Sale Facility` ELSE NULL END AS SELLING_ENTITY,
    'InParkOutlet' AS CHANNEL,
    ig.`Outlet Name` AS CHANNEL_AREA,

    -- Market Segment RELATED (Get more info on DMG)
    CAST(NULL AS STRING) AS DMG_CATEGORY,
    CAST(NULL AS STRING) AS DMG_NAME,
    CAST(NULL AS STRING) AS ANNUAL_PASS_DMG,
    CASE
        WHEN LENGTH(ig.`Promotion DMG`) > 3 AND LOWER(ig.`Promotion DMG`) NOT IN ('na', "not available", "not applicable")
        THEN ig.`Promotion DMG`
        ELSE NULL
    END AS PROMOTOIN_DMG,
    CASE
        WHEN LENGTH(ig.`Promotion DMG Description`) > 1 AND LOWER(ig.`Promotion DMG Description`) NOT IN ('na', "not available", "not applicable")
        THEN ig.`Promotion DMG Description`
        ELSE NULL
    END AS PROMOTOIN_DMG_DESCRIPTION,
    CAST(NULL AS STRING) AS MARKET_SEGMENT_GROUP1,
    CAST(NULL AS STRING) AS MARKET_SEGMENT_GROUP2,
    CAST(NULL AS STRING) AS 3RDPARTY_MARKET_SEGMENT,
    CAST(NULL AS STRING) AS 3RDPARTY_MARKET_REGION,

    -- PROMOTION RELATED
    CASE
        WHEN LENGTH(ig.`OMNI Promotion Name`) > 1 AND LOWER(ig.`OMNI Promotion Name`) NOT IN ("not available", "not applicable")
        THEN ig.`OMNI Promotion Name`
        ELSE NULL
    END AS PROMOTION_NAME,
    CASE WHEN LENGTH(ig.`Promotion Type`) > 1 THEN ig.`Promotion Type` ELSE NULL END AS PROMOTION_TYPE,
    CASE WHEN LENGTH(ig.`Promotion Classification`) > 1 THEN ig.`Promotion Classification` ELSE NULL END AS PROMOTION_CLASS,
    ig.`Promotion Valid From` AS PROMOTION_FROM_DATE,
    ig.`Promotion Valid TO` AS PROMOTION_TO_DATE,
    CAST(NULL AS STRING) AS COMPLEMENTRY_ITEM,

    -- SALE PRICES RELATED
    ig.`Gross Sale Amount` AS GROSS_SALE_AMOUNT,
    CASE WHEN LENGTH(ig.`Discount Amount`) > 0 THEN ig.`Discount Amount` ELSE NULL END AS DISCOUNT_AMOUNT,
    ig.`Net Sale Amount (-Tax)` AS NET_SALE_AMOUNT_NO_TAX,
    ig.`Net Sale Amount` AS NET_SALE_AMOUNT,
    ig.`Revenue Stream / GL Group Name` AS REVENUE_TYPE,
    ig.`Transaction Type` AS TRANSACTION_TYPE
FROM ig_gold AS ig
""")
#asset name standardization
df_transactions_ig_raw = map_asset_category(df_transactions_ig_raw,"SELLING_ENTITY")

df_transactions_ig_raw.createOrReplaceTempView("transactions_ig_raw")

# COMMAND ----------

# DBTITLE 1,final data
df_transactions_ig = spark.sql(f"""
SELECT
    map('scv_sk', cast(coalesce(scv.scv_account_sk,IFF(scv2.dob=omni.dob or omni.dob is null or scv2.dob is null,scv2.scv_account_sk,NULL) )  as string),
     'omni_sk', cast(coalesce(omni.omni_account_sk , IFF(omni2.dob=scv.dob or omni2.dob is null or scv.dob is null,omni2.omni_account_sk,NULL)) as string),
     'email_id', cast(coalesce(scv.email_id,omni.email_id) as string)
     ) as entity_identifiers,
    coalesce(scv.scv_account_sk,IFF(scv2.dob=omni.dob or omni.dob is null or scv2.dob is null,scv2.scv_account_sk,NULL)) as purchaser_scv_sk,
    coalesce(omni.omni_account_sk , IFF(omni2.dob=scv.dob or omni2.dob is null or scv.dob is null,omni2.omni_account_sk,NULL) ) as purchaser_omni_sk,
    purchaser_scv_sk as owner_scv_sk,
    purchaser_omni_sk as owner_omni_sk,
    CASE WHEN lower(PROMOTION_NAME) like 'fp-%' THEN scv.scv_account_sk ELSE NULL END as fl_scv_sk,

    case 
        when regexp_like(lower(PROMOTION_NAME), r'(coll|office|miral|duty|staff)') and (scv.dependent_flag = 'Y' or omni.dependent_flag = 'Y') and lower(PROMOTION_NAME) like 'fp-%'  then 'Colleague Dependents - FP'
        when regexp_like(lower(PROMOTION_NAME), r'(coll|office|miral|duty|staff)') and (scv.dependent_flag = 'Y' or omni.dependent_flag = 'Y') and lower(PROMOTION_NAME) not like 'fp-%'  then 'Colleague Dependents'
        when regexp_like(lower(PROMOTION_NAME), r'(coll|office|miral|duty|staff)') and lower(PROMOTION_NAME) like 'fp-%' then 'Miral Colleagues - FP' 
        when regexp_like(lower(PROMOTION_NAME), r'(coll|office|miral|duty|staff)') and lower(PROMOTION_NAME) not like 'fp-%' then 'Miral Colleagues' 
        when annual_pass = 'Y' and lower(PROMOTION_NAME) like 'fp-%'  then 'Annual Pass - FP'
        when annual_pass = 'Y' and lower(PROMOTION_NAME) not like 'fp-%'  then 'Annual Pass'  
        when lower(PROMOTION_NAME) like 'fp-%' or lower(PROMOTION_NAME) like '%facepass%' then 'B2C - FP'
        else 'B2C' 
    end as customer_type,

    b2b_account_id,
    account_manager_name,

    -- sale related
    sale_id,
    sale_timestamp,
    transaction_timestamp,
    sale_source_system,

    -- ticket related
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

    -- 3rd party sale related
    3rdparty_sale_product_category,
    3rdparty_sale_booking_type,
    3rdparty_sale_booking_status,
    3rdparty_sale_payment_status,
    3rdparty_sale_cancelation_date,

    -- product related
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

    -- hotel related
    hotel_name,
    hotel_number_rooms,
    hotel_number_nights,
    hotel_length_stay,

    -- sales channel related
    partner_name,
    selling_entity,
    channel,
    channel_area,

    -- market segment related
    dmg_category,
    dmg_name,
    annual_pass_dmg,
    promotoin_dmg,
    promotoin_dmg_description,
    market_segment_group1,
    market_segment_group2,
    3rdparty_market_segment,
    3rdparty_market_region,

    -- promotion related
    promotion_name,
    promotion_type,
    promotion_class,
    promotion_from_date,
    promotion_to_date,
    complementry_item,

    -- sale prices related
    gross_sale_amount,
    discount_amount,
    net_sale_amount_no_tax,
    net_sale_amount,
    revenue_type,
    transaction_type,
    case when scv.scv_account_sk is null and
            omni.omni_account_sk is null  then 'N'
            else 'Y' end as valid_sk,  -- all sk are null then N else Y
    sale_source_system as source_name,
    '{current_uae_timestamp}' as etl_created_datetime,
    '{current_uae_timestamp}' as etl_updated_datetime,

      map(
      'transaction_id',cast(ig.Transaction_id as string),
      'memo_yas_id', cast(ig.Memo_Yas_ID as string),
      'memo_account_id',cast( ig.Memo_Account_ID as string),
      'memo_text',cast( ig.memo_text as string)
   ) as others

        
FROM transactions_ig_raw AS ig

left join {scv_account_master} scv 
on scv.yas_id = ig.Memo_Yas_ID AND scv.active_yas_id_flag = 'Y'

left join {omni_account_master} omni 
on omni.account_id = ig.Memo_Account_ID

left join {scv_omni_link} link_scv_omni 
on link_scv_omni.scv_account_sk = scv.scv_account_sk AND link_scv_omni.scv_rank = 1

left join {scv_omni_link} link_omni_scv
on link_omni_scv.omni_account_sk = omni.omni_account_sk AND link_omni_scv.omni_rank = 1

left join {omni_account_master} omni2
on omni2.omni_account_sk = link_scv_omni.omni_account_sk

left join {scv_account_master} scv2
on scv2.scv_account_sk = link_omni_scv.scv_account_sk
""")

df_transactions_ig.createOrReplaceTempView("transactions_ig")

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_transactions_ig = apply_schema_casting(df_transactions_ig,transactions_schema )

# COMMAND ----------

# DBTITLE 1,Write to UC
# Build DDL string from schema dict
transactions_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in transactions_schema.items()
)
 
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(transactions_ddl)
    if compare_schema_lenient(df_transactions_ig.schema, ddl_struct):
        print("Schema matches!")

        deleted_count = 0
        inserted_count = 0

        # Check table state
        table_exists = spark.catalog.tableExists(target_table)
        has_ig_data = table_exists and spark.sql(f"SELECT 1 FROM {target_table} WHERE SALE_SOURCE_SYSTEM = 'IG' LIMIT 1").count() > 0

        # Load strategy: delete old IG data for incremental
        if has_ig_data:
            if start_date and end_date and start_date < end_date: 
                delete_result = spark.sql(f"""
                                          DELETE FROM {target_table} 
                                          WHERE cast(SALE_TIMESTAMP as date) BETWEEN '{start_date}' AND '{end_date}' 
                                          AND SALE_SOURCE_SYSTEM = 'IG'
                                          """)
                deleted_count = delete_result.collect()[0]['num_affected_rows']
            else:
                delete_result = spark.sql(f"""
                                          DELETE FROM {target_table} 
                                          WHERE cast(SALE_TIMESTAMP as date) >= '{timestamp}' 
                                          AND SALE_SOURCE_SYSTEM = 'IG'
                                          """)
                deleted_count = delete_result.collect()[0]['num_affected_rows']
            print("Incremental Load")
        else:
            print("Full Load")

        # Ensure table exists with partitioning — zero-cost (empty DataFrame, schema only)
        if not table_exists:
            try:
                (spark.createDataFrame([], df_transactions_ig.schema)
                    .write.format("delta")
                    .saveAsTable(target_table))
            except AnalysisException:
                print("Table was created concurrently -- appending instead.")
                pass  # Table was created concurrently — safe to continue
            
        # Single data write — always exactly one append
        (df_transactions_ig
            .write.format("delta")
            .mode("append")
            .saveAsTable(target_table))

        # Get inserted row count from Delta table history
        history_metrics = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").select("operationMetrics").collect()[0][0]
        inserted_count = int(history_metrics.get("numOutputRows", 0))

        print("Transactions IG Data Loaded")
        print(f"Rows Deleted: {deleted_count}")
        print(f"Rows Inserted: {inserted_count}")
        
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_transactions_ig.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,tbl readiness - update tag
readiness_table = f"transformation_{env}.plt_customer360.readiness_transactions_ig"

df = spark.createDataFrame([(spark.sql("SELECT from_utc_timestamp(current_timestamp(), 'Asia/Dubai')").collect()[0][0],)],["last_updated_timestamp"])
display(df)

df.write.mode("append").saveAsTable(readiness_table)
add_last_updated_tag(readiness_table)

# COMMAND ----------

# DBTITLE 1,trigger orchestration
trigger_databricks_job_by_name('workflow_orchestration')

# COMMAND ----------

# DBTITLE 1,log stats
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
 
