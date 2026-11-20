# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC This notebook processes sales and attendance data, enforces schema consistency, and incrementally loads processed results into the `transactions` table for analytics.
# MAGIC 1. Source Gold Table: 
# MAGIC  Schema | Table |
# MAGIC  --- | --- |
# MAGIC gld_travelbox |dwh_vw_sales |
# MAGIC  gld_drr |tbvw_sales_transactions|
# MAGIC  slv_facepassreporting |slv_lkp_frictionless_data_tickets_snapshot|
# MAGIC  slv_facepassreporting |slv_lkp_frictionless_data_account_snapshot|
# MAGIC
# MAGIC 2. Target Platinum table: **transactions**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 
# MAGIC
# MAGIC 4. Load Strategy: **Incremental Load (18 months)**

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
vw_sales = f"transformation_{env}.gld_travelbox.dwh_vw_sales"
sales_transactions = f"transformation_{env}.gld_drr.tbvw_sales_transactions"
company = f"transformation_{env}.gld_shared.dwh_dim_company"
data_ticket_table = f"transformation_{env}.slv_facepassreporting.slv_lkp_frictionless_data_tickets_snapshot"

#target table
target_table = f"transformation_{env}.plt_customer360.transactions"
#master tables
scv_account_master = f"transformation_{env}.plt_customer360.scv_account_master"
omni_account_master = f"transformation_{env}.plt_customer360.omni_account_master"


current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")



print(f"Source table: {sales_transactions}")
print(f"Source tables: {vw_sales}")
print(f"Source Table: {data_ticket_table}")
print(f"Target table: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,filter condition
incremental_orphan_filter = f"""CASE WHEN (tb.`Theme Park Flag` = 'Y' OR tb.`Park Flag` = 'Y') 
                                                AND omni.`Transaction Date Time` IS NULL 
                                                THEN 0
                                                ELSE 1
                                    END = 1 """
if (not spark.catalog.tableExists(target_table)) or (spark.sql(f"SELECT 1 FROM {target_table} WHERE SALE_SOURCE_SYSTEM = 'OMNI' OR SALE_SOURCE_SYSTEM = 'TRAVEL_BOX' LIMIT 1").count() == 0):
    print("Table not exists or OMNI/TRAVELBOX data not present,Full Load")
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
print(f"Incremental Orphan Filter : {incremental_orphan_filter}")


# COMMAND ----------

# DBTITLE 1,transactions_tickets
df_transactions_omni_raw = spark.sql(f"""
  with rank as (    
    select omni_silver.* except(omni_silver.`Unit Price After discount`,
                              omni_silver.`Unit Price before discount`, 
                              omni_silver.`Tender Amount (in AED)`, 
                              omni_silver.`Tax Amount`,
                              omni_silver.`Quantity`,
                              omni_silver.`Net Sale Amount`,
                              omni_silver.`Net Sale Amount (-Tax)`,
                              omni_silver.`Gross Sale Amount`, 
                              omni_silver.`Discount Quantity`,
                              omni_silver.`Discount Amount`, 
                              omni_silver.`Discount Amount(incl VAT)`)
      , omni_silver.`Email` AS EMAIL_ADDRESS -- check this logic
      ,row_number() over(partition by omni_silver.`Ticket ID` order by omni_silver.`Transaction Date Time` desc) as row_n
      ,sum(omni_silver.`Unit Price After discount`)     over(partition by omni_silver.`Ticket ID`) as `Unit Price After discount` 
      ,sum(omni_silver.`Unit Price before discount`)    over(partition by omni_silver.`Ticket ID`) as `Unit Price before discount`
      ,sum(omni_silver.`Tender Amount (in AED)`)        over(partition by omni_silver.`Ticket ID`) as `Tender Amount (in AED)`
      ,sum(omni_silver.`Tax Amount`)                    over(partition by omni_silver.`Ticket ID`) as `Tax Amount` 
      ,sum(omni_silver.`Quantity`)                      over(partition by omni_silver.`Ticket ID`) as `Quantity` 
      ,sum(omni_silver.`Net Sale Amount`)               over(partition by omni_silver.`Ticket ID`) as `Net Sale Amount` 
      ,sum(omni_silver.`Net Sale Amount (-Tax)`)        over(partition by omni_silver.`Ticket ID`) as `Net Sale Amount (-Tax)` 
      ,sum(omni_silver.`Gross Sale Amount`)             over(partition by omni_silver.`Ticket ID`) as `Gross Sale Amount` 
      ,sum(omni_silver.`Discount Quantity`)             over(partition by omni_silver.`Ticket ID`) as `Discount Quantity` 
      ,sum(omni_silver.`Discount Amount`)               over(partition by omni_silver.`Ticket ID`) as `Discount Amount`
      ,sum(omni_silver.`Discount Amount(incl VAT)`)     over(partition by omni_silver.`Ticket ID`) as `Discount Amount(incl VAT)`

  from {sales_transactions} omni_silver
  where omni_silver.`Source Name` = 'OMNI_NEW'
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
    {yas_id_standardization("`AP MYPASS ID`")} AS `AP MYPASS ID`,
     (case when `Unit Price After discount` < 0 then 0 else `Unit Price After discount` end ) as `Unit Price After discount`,
     (case when `Unit Price before discount` < 0 then 0 else `Unit Price before discount` end ) as `Unit Price before discount`,
     (case when `Tender Amount (in AED)` < 0 then 0 else `Tender Amount (in AED)` end ) as `Tender Amount (in AED)`,
     (case when `Tax Amount` < 0 then 0 else `Tax Amount` end ) as `Tax Amount`,
     (case when `Quantity` < 0 then 0 else `Quantity` end ) as `Quantity`,
     (case when `Net Sale Amount` < 0 then 0 else `Net Sale Amount` end ) as `Net Sale Amount`,
     (case when `Net Sale Amount (-Tax)` < 0 then 0 else `Net Sale Amount (-Tax)` end ) as `Net Sale Amount (-Tax)`,
     (case when `Gross Sale Amount` < 0 then 0 else `Gross Sale Amount` end ) as `Gross Sale Amount`, 
     (case when `Discount Quantity` < 0 then 0 else `Discount Quantity` end ) as `Discount Quantity`,
     (case when `Discount Amount` < 0 then 0 else `Discount Amount` end ) as `Discount Amount`,
     
     iff(rank.`AP Account ID` != rank.`OMNI Account ID` and lower(trim(rank.`AP MYPASS ID`)) = lower(trim(rank.`YAS ID`)) ,0,1) as valid_ap_yas_id_flag

  from rank 
  where row_n = 1 and `Ticket ID` is not null
""")

df_transactions_omni_raw.createOrReplaceTempView("transactions_omni_raw")

# COMMAND ----------

# DBTITLE 1,hotel tickets
df_transactions_hotel_tickets = spark.sql(f"""
      select 
      tb.`Booking No`,
      tb.`Ticket ID`,
      tb.`Email address`,
      {email_standardization("tb.`Email address`")} AS tb_EMAIL_ADDRESS,
      tb.`Booking Creation Date`,
      tb.`Theme Park Flag`,
      tb.`Park Flag`,
      tb.`Hotel Flag`,
      tb.`Booking Type`,
      tb.`Booking Status`,
      tb.`Option Status`,
      tb.`Cancellation Date`,
      CAST(tb.`Product ID` as STRING),
      tb.`Product Name`,
      tb.`Booking Item Reference`,
      tb.`Gross Revenue`,
      tb.`Cost of Item`,
      tb.`Promotions Discount Amount`,
      tb.`Hotel Name B2B Hotel`,
      tb.`Hotel Name`,
      tb.`Number of Rooms`,
      tb.`Room Nights`,
      tb.`Length of Stay`,
      tb.`Partner Name`,
      tb.`Selling Company`,
      tb.`Sub Channels`,
      tb.`Segments`,
      tb.`Region`
      from {vw_sales} tb
      WHERE 1=1
            AND (tb.`Distribution Channel` IS NULL OR tb.`Distribution Channel` NOT IN ('Do Not Use'))
            AND (tb.`Partner Name` IS NULL OR tb.`Partner Name` NOT IN (
                  'YIhotelstest','Yasproducttestaccount','Alexander Test','cgTest',
                  'EH TEST HOTEL','MD Alameri TEST','PROD TEST','Rebin Test',
                  'Rebin Test Sub User','Sophia Test','Test Hotel','Test Karin Seifert'
            ))
            AND (tb.`Hotel Name` IS NULL OR tb.`Hotel Name` NOT IN (
                  'Yas Generic Test','Test Supplier','TEST Hotel'
            ))
            --and `Theme Park Flag` = 'Y'
            --and `Ticket ID` is not null
            and `Theme Park Flag` is not null 
            and `Hotel Flag` is not null
            and `Booking No` is not null
qualify (row_number() over (partition by `Ticket ID`,`Booking No` order by `Booking Creation Date` desc)) = 1
""")

df_transactions_hotel_tickets.createOrReplaceTempView("hotel_tickets")

# COMMAND ----------

# DBTITLE 1,Transformation
df_transactions_omni = spark.sql(f"""
WITH fp_base as (
select
    frictionless_data_tickets.mediaidentifier
    ,frictionless_data_tickets.mypassid

from {data_ticket_table} frictionless_data_tickets
qualify( row_number() over (partition by frictionless_data_tickets.mediaidentifier order by frictionless_data_tickets.updated desc,frictionless_data_tickets.snapdate desc)) = 1
),parent_scv as (
    select 
    email_id,scv_account_sk 
    from {scv_account_master}
    where source_name = 't_mdm_guest'
    QUALIFY row_number() over(partition by email_id order by modified_date desc) = 1
)
SELECT
    IFF(scv_tb.scv_account_sk is not null, scv_tb.scv_account_sk, scv.scv_account_sk) as purchaser_scv_sk,
    IFF(scv_tb.scv_account_sk is not null, NULL,omni_acc.omni_account_sk) as purchaser_omni_sk,
    
    IFF(scv_tb.scv_account_sk is not null, scv_tb.scv_account_sk, scv2.scv_account_sk) as owner_scv_sk,
    IFF(scv_tb.scv_account_sk is not null, NULL, omni_acc2.omni_account_sk ) as owner_omni_sk,

    scv3.scv_account_sk AS fl_scv_sk,

    `Transaction ID` as transaction_id,
    omni.`YAS ID`  as yas_id,
    omni.`AP MYPASS ID`  as ap_mypass_id,
    omni.`OMNI Account ID` as  omni_account_id,
    omni.`AP Account ID` as  ap_account_id,
    omni.`Media Code` as media_code,

    tb.`Booking No` as tb_booking_no,
    tb.tb_EMAIL_ADDRESS,

    CASE
        WHEN length(omni.`Partner Company Account Id (OMNI)`) > 1 THEN omni.`B2B Account ID`
        ELSE NULL
    END AS B2B_ACCOUNT_ID,
    CASE
        WHEN  ( scv2.dependent_flag = "Y" or omni_acc2.dependent_flag = "Y" or  scv3.dependent_flag = "Y" ) THEN 'Colleague Dependents'
        WHEN ( scv.staff_flag = "Y" or omni_acc.staff_flag = "Y" ) and ( ifnull(scv2.staff_flag,'N') = "N" and ifnull(omni_acc2.staff_flag,'N') = "N" and ifnull(scv3.staff_flag,'N') = "N" ) then 'Colleague F&F'
        WHEN scv.staff_flag = "Y" OR omni.`Market Segment Group 1` = 'Miral Colleagues' THEN 'Miral Colleagues'
        WHEN omni.`Annual Pass Flag` = 'Y' then 'Annual Pass'  
        WHEN omni.`Market Segment Group 1` IN ('T&T', 'Corporate and Government', 'Partner Hotels', 'School Groups', 'Sponsors and Partners') THEN 'B2B'
        WHEN omni.`Market Segment Group 1` IN ('Packages') AND length(omni.`B2B Account ID`) > 1 THEN 'B2B'
        WHEN omni.`Market Segment Group 1` IN ('General Public') THEN 'B2C'
        WHEN omni.`Market Segment Group 1` = 'Packages' AND omni.`B2B Account ID` IS NULL THEN 'B2B&B2C'
        WHEN length(tb.`Booking No`) > 0 then 'B2B'
    END AS CUSTOMER_TYPE,
    
    map('scv_sk',  IFF(scv_tb.scv_account_sk is not null, scv_tb.scv_account_sk, IFF(CUSTOMER_TYPE in ('Colleague F&F','B2B'), scv2.scv_account_sk, scv.scv_account_sk)),
     'omni_sk', IFF(scv_tb.scv_account_sk is not null, NULL, IFF(CUSTOMER_TYPE in ('Colleague F&F','B2B'),  omni_acc2.omni_account_sk , omni_acc.omni_account_sk)),
     'email_id',  IFF(scv_tb.scv_account_sk is not null, tb.tb_EMAIL_ADDRESS,  IFF(CUSTOMER_TYPE in ('Colleague F&F','B2B'), coalesce(scv2.email_id,omni_acc2.email_id), coalesce(scv.email_id,omni_acc.email_id) ))
        ) as entity_identifiers,

    omni.`Account Manager Name` AS ACCOUNT_MANAGER_NAME,
    CASE
        WHEN length(tb.`Booking No`) > 0 THEN tb.`Booking No`
        ELSE omni.`Sale ID/Check Number`
    END AS SALE_ID,
    CASE
        WHEN length(tb.`Booking No`) > 0 THEN tb.`Booking Creation Date`
        ELSE omni.`Transaction Date Time`
    END
    AS SALE_TIMESTAMP,
    omni.`Transaction Date Time`    AS TRANSACTION_TIMESTAMP,  -- new column for incremental implementation
    upper(
        CASE
            WHEN length(tb.`Booking No`) > 0 THEN "TRAVEL_BOX"
            ELSE "OMNI"
        END
    ) AS SALE_SOURCE_SYSTEM,
    CASE
        WHEN length(omni.`Ticket ID`) > 0 THEN omni.`Ticket ID`
        ELSE tb.`Ticket ID`
    END AS TICKET_ID,
    omni.`Void Type Description` AS TICKET_STATUS,
    CASE
        WHEN lower(omni.`Annual Pass Flag`) NOT IN ("not available", "not applicable") THEN omni.`Annual Pass Flag`
        ELSE NULL
    END AS ANNUAL_PASS,
    CASE
        WHEN lower(omni.`OMNI Multipark Flag`) NOT IN ("not available", "not applicable") THEN omni.`OMNI Multipark Flag`
        ELSE NULL
    END AS MULTI_PARK_PASS,
    -- CASE
    --     WHEN length(omni.`Yas Park Pass`) > 1 THEN omni.`Yas Park Pass`
    --     ELSE NULL
    -- END AS YAS_PARK_PASS,
    CASE
        WHEN lower(omni.`Ticket Tier`) != "unspecified" THEN omni.`Ticket Tier`
        ELSE NULL
    END AS TICKET_TIER,
    CASE
        WHEN lower(omni.`Ticket Age`) != "unspecified" THEN omni.`Ticket Age`
        ELSE NULL
    END AS TICKET_AGE_CATEGORY,
    omni.`Ticket Active From` AS TICKET_VALID_FROM,
    omni.`Ticket Active To` AS TICKET_VALID_TO,
    CASE
        WHEN lower(omni.`Used Flag`) NOT IN ("not available", "not applicable") THEN omni.`Used Flag`
        ELSE NULL
    END AS TICKET_IS_USED,
    omni.`Ticket Usage Date` AS TICKET_USAGE_DATE,
    CASE
        WHEN tb.`Theme Park Flag` = 'Y' AND tb.`Park Flag` = 'Y' THEN 'THEME PARK & PARK'
        WHEN tb.`Park Flag` = "Y" AND tb.`Theme Park Flag` = 'N' THEN 'PARK ONLY'
        WHEN tb.`Hotel Flag` = "Y" THEN "HOTEL ONLY"
        ELSE NULL
    END AS 3rdPARTY_SALE_PRODUCT_CATEGORY,
    tb.`Booking Type` AS 3rdPARTY_SALE_BOOKING_TYPE,
    tb.`Booking Status` AS 3rdPARTY_SALE_BOOKING_STATUS,
    tb.`Option Status` AS 3rdPARTY_SALE_PAYMENT_STATUS,
    tb.`Cancellation Date` AS 3rdPARTY_SALE_CANCELATION_DATE,
    CAST(CASE
        WHEN length(tb.`Booking No`) > 0 THEN tb.`Product ID`
        WHEN length(omni.`Product Code`) > 1 THEN omni.`Product Code`
        ELSE NULL
    END AS STRING) AS PRODUCT_ID,
    CASE
        WHEN length(tb.`Booking No`) > 0 THEN tb.`Product Name`
        WHEN length(omni.`Product Name`) > 1 THEN omni.`Product Name`
        ELSE NULL
    END AS PRODUCT_NAME,
    CASE
        WHEN tb.`Hotel Flag` = "Y" THEN 'Hotel'
        WHEN length(tb.`Booking No`) > 0 THEN tb.`Booking Item Reference`
        WHEN length(omni.`Product Category Name`) > 1 THEN omni.`Product Category Name`
        ELSE NULL
    END AS PRODUCT_CATEGORY,
    omni.`Product Sub Category Name` AS PRODUCT_SUBCATEGORY,
    CASE
        WHEN lower(omni.`OMNI Park Combination`) NOT IN ("not available", "not applicable") THEN omni.`OMNI Park Combination`
        ELSE NULL
    END AS PARK_TYPE,
    omni.`Quantity` AS QUANTITY,
    CASE
        WHEN length(tb.`Booking No`) > 0 AND tb.`Booking Item Reference` = "Hotels" AND tb.`Booking Status` = "CONFIRMED" THEN round(tb.`Gross Revenue`/(tb.`Number of Rooms` * tb.`Room Nights`), 2)
        WHEN length(tb.`Booking No`) > 0 AND tb.`Booking Item Reference` = "Generic" AND tb.`Booking Status` = "CONFIRMED" THEN tb.`Cost of Item`
        WHEN omni.`Unit Price before discount` IS NOT NULL THEN omni.`Unit Price before discount`
        ELSE NULL
    END AS UNIT_PRICE,
    CASE
        WHEN length(tb.`Booking No`) > 0 AND tb.`Booking Item Reference` = "Hotels" AND tb.`Booking Status` = "CONFIRMED" THEN round(tb.`Gross Revenue`/(tb.`Number of Rooms` * tb.`Room Nights`), 2)
        WHEN length(tb.`Booking No`) > 0 AND tb.`Booking Item Reference` = "Generic" AND tb.`Booking Status` = "CONFIRMED" THEN tb.`Cost of Item`
        WHEN omni.`Unit Price After discount` IS NOT NULL THEN omni.`Unit Price After discount`
        ELSE NULL
    END AS DISCOUNTED_UNIT_PRICE,
    CASE
        WHEN tb.`Hotel Flag` = "Y" THEN coalesce(tb.`Hotel Name B2B Hotel`, tb.`Hotel Name`)
        ELSE NULL
    END AS HOTEL_NAME,
    CASE
        WHEN tb.`Number of Rooms` != 0 AND tb.`Hotel Flag` = "Y" THEN tb.`Number of Rooms`
        ELSE NULL
    END AS HOTEL_NUMBER_ROOMS,
    CASE
        WHEN tb.`Room Nights` != 0 AND tb.`Hotel Flag` = "Y" THEN tb.`Room Nights`
        ELSE NULL
    END AS HOTEL_NUMBER_NIGHTS,
    CASE
        WHEN tb.`Length of Stay` != 0 AND tb.`Hotel Flag` = "Y" THEN tb.`Length of Stay`
        ELSE NULL
    END AS HOTEL_LENGTH_STAY,
    CASE
        WHEN length(omni.`Partner Company Account Name (OMNI)`) > 1 AND lower(omni.`Partner Company Account Name (OMNI)`) NOT IN ("not available", "not applicable") THEN omni.`Partner Company Account Name (OMNI)`
        ELSE tb.`Partner Name`
    END AS PARTNER_NAME,
    CASE
        WHEN length(omni.`Ticket Sale Facility`) > 1 THEN omni.`Ticket Sale Facility`
        WHEN length(tb.`Selling Company`) > 0 THEN tb.`Selling Company`
        ELSE NULL
    END AS SELLING_ENTITY,
    CASE
        WHEN omni.`WorkStation Channel` IS NOT NULL THEN omni.`WorkStation Channel`
        ELSE tb.`Sub Channels`
    END AS CHANNEL,
    omni.`WorkStation Area` AS CHANNEL_AREA,
    omni.`OMNI Account Parent Category DMG Description` AS DMG_CATEGORY,
    omni.`Partner Account DMG Description (OMNI)` AS DMG_NAME,
    omni.`AP DMG DESC` AS ANNUAL_PASS_DMG,
    CASE
        WHEN length(omni.`Promotion DMG`) > 1 AND lower(omni.`Promotion DMG`) NOT IN ('na', "not available", "not applicable") THEN omni.`Promotion DMG`
        ELSE NULL
    END AS PROMOTOIN_DMG,
    CASE
        WHEN length(omni.`Promotion DMG Description`) > 1 AND lower(omni.`Promotion DMG Description`) NOT IN ('na', "not available", "not applicable") THEN omni.`Promotion DMG Description`
        ELSE NULL
    END AS PROMOTOIN_DMG_DESCRIPTION,
    omni.`Market Segment Group 1` AS MARKET_SEGMENT_GROUP1,
    omni.`Market Segment Group 2` AS MARKET_SEGMENT_GROUP2,
    CASE
        WHEN lower(tb.`Segments`) NOT IN ("not available", "not applicable") AND tb.`Segments` IS NOT NULL THEN tb.`Segments`
        ELSE omni.`Market Segment Group 1`
    END AS 3RDPARTY_MARKET_SEGMENT,
    tb.`Region` AS 3RDPARTY_MARKET_REGION,
    CASE
        WHEN lower(omni.`OMNI Promotion Name`) NOT IN ("not available", "not applicable") THEN omni.`OMNI Promotion Name`
        ELSE NULL
    END AS PROMOTION_NAME,
    CASE
        WHEN length(omni.`Promotion Type`) > 1 THEN omni.`Promotion Type`
        ELSE NULL
    END AS PROMOTION_TYPE,
    CASE
        WHEN length(omni.`Promotion Classification`) > 1 THEN omni.`Promotion Classification`
        ELSE NULL
    END AS PROMOTION_CLASS,
    omni.`Promotion Valid From` AS PROMOTION_FROM_DATE,
    omni.`Promotion Valid TO` AS PROMOTION_TO_DATE,
    CASE
        WHEN lower(omni.`Complimentary Product Flag`) NOT IN ("not available", "not applicable") THEN omni.`Complimentary Product Flag`
        ELSE NULL
    END AS COMPLEMENTRY_ITEM,
    CASE
        WHEN omni.`Gross Sale Amount` IS NOT NULL THEN omni.`Gross Sale Amount`
        WHEN length(tb.`Booking No`) > 0 AND tb.`Gross Revenue` IS NOT NULL THEN tb.`Gross Revenue`
        ELSE NULL
    END AS GROSS_SALE_AMOUNT,
    CASE
        WHEN length(omni.`Discount Amount`) > 0 THEN omni.`Discount Amount`
        WHEN length(tb.`Promotions Discount Amount`) > 0 THEN tb.`Promotions Discount Amount`
        ELSE NULL
    END AS DISCOUNT_AMOUNT,
    omni.`Net Sale Amount (-Tax)` AS NET_SALE_AMOUNT_NO_TAX,
    CASE
        WHEN omni.`Net Sale Amount` IS NOT NULL THEN omni.`Net Sale Amount`
        WHEN length(tb.`Booking No`) > 0 AND tb.`Gross Revenue` IS NOT NULL THEN tb.`Gross Revenue`
        ELSE NULL
    END AS NET_SALE_AMOUNT,
    CASE
        WHEN length(tb.`Booking No`) > 0 and tb.`Theme Park Flag` = 'Y' THEN 'Total Package Ticket Revenue'
        WHEN length(tb.`Booking No`) > 0 and tb.`Hotel Flag` = 'Y' THEN 'Total Package Hotel Revenue'
        WHEN length(tb.`Booking No`) > 0 and tb.`Theme Park Flag` = 'N' AND tb.`Hotel Flag` = 'N' THEN 'Total Package Other Revenue'
        WHEN length(tb.`Booking No`) = 0 AND omni.`Revenue Stream / GL Group Name` = 'Hotel' THEN 'Hotel'
        ELSE omni.`Revenue Stream / GL Group Name`
    END AS REVENUE_TYPE,
    omni.`Transaction Type` AS TRANSACTION_TYPE

FROM transactions_omni_raw AS omni

FULL OUTER JOIN hotel_tickets AS tb -- full load
ON omni.`TICKET ID` = tb.`TICKET ID` AND omni.`EH BOOKING ID` = tb.`BOOKING NO`

left join fp_base fp 
ON fp.mediaidentifier::string = omni.`Media Code`::string


left join {scv_account_master} scv3 
on scv3.yas_id = fp.mypassid AND scv3.active_yas_id_flag = 'Y'

left join parent_scv scv_tb 
on  scv_tb.email_id = tb.tb_EMAIL_ADDRESS

left join {omni_account_master} omni_acc 
on omni_acc.account_id = omni.`OMNI Account ID`

left join {omni_account_master} omni_acc2 
on omni_acc2.account_id = omni.`AP Account ID`

left join {scv_account_master} scv 
on scv.yas_id = omni.`YAS ID` 
   AND scv.active_yas_id_flag = 'Y'
   AND scv.email_id = omni_acc.email_id 
   and (scv.dob = omni_acc.dob or scv.dob is null or omni_acc.dob is null)

left join {scv_account_master} scv2 
on scv2.yas_id = omni.`AP MYPASS ID` 
   AND scv2.active_yas_id_flag = 'Y' 
   AND omni.valid_ap_yas_id_flag = 1
   AND scv2.email_id = omni_acc2.email_id 
   and (scv2.dob = omni_acc2.dob or scv2.dob is null or omni_acc2.dob is null)

WHERE (length(omni.`B2B Account ID`) > 1 
OR  omni.`YAS ID` is not NULL
OR omni.`AP MYPASS ID` is not NULL
OR omni.`OMNI Account ID` is not NULL
OR omni.`AP Account ID` is not NULL
OR omni.`Media Code` is not NULL 
OR tb.`Booking No` is not NULL)
AND {incremental_orphan_filter} -- remove records from travel box dataset which are orphan due to omni date range

""")
#asset name standardization
df_transactions_omni = map_asset_category(df_transactions_omni,"SELLING_ENTITY")
df_transactions_omni.createOrReplaceTempView("transactions_omni")

# COMMAND ----------

# DBTITLE 1,fianl data product

df_transactions_omni = spark.sql(f"""
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
   sale_source_system as source_name,
   '{current_uae_timestamp}' as etl_created_datetime,
   '{current_uae_timestamp}' as etl_updated_datetime,
     map(
      'transaction_id',cast(Transaction_id as string),
      'yas_id',cast(yas_id as string),
      'ap_mypass_id',cast(ap_mypass_id as string),
      'omni_account_id',cast(omni_account_id as string),
      'ap_account_id',cast(ap_account_id as string),
      'media_code',cast(media_code as string),
      'travelbox_booking_no',cast(tb_booking_no as string),
      'travelbox_email_id',cast(tb_EMAIL_ADDRESS as string)

   ) as others
  from transactions_omni 
  """)

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_transactions_omni = apply_schema_casting(df_transactions_omni,transactions_schema )

# COMMAND ----------

# DBTITLE 1,Write to UC
# Build DDL string from schema dict
transactions_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in transactions_schema.items()
)
 
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(transactions_ddl)
    if compare_schema_lenient(df_transactions_omni.schema, ddl_struct):
        print("Schema matches!")

        deleted_count = 0
        inserted_count = 0

        # Check table state
        table_exists = spark.catalog.tableExists(target_table)
        has_omni_data = table_exists and spark.sql(f"SELECT 1 FROM {target_table} WHERE SALE_SOURCE_SYSTEM = 'OMNI' OR SALE_SOURCE_SYSTEM = 'TRAVEL_BOX' LIMIT 1").count() > 0

        # Load strategy: Incremental using TRANSACTION_TIMESTAMP (= omni.Transaction Date Time)
        # TRANSACTION_TIMESTAMP aligns with source filter for BOTH OMNI and TRAVEL_BOX records
        # because TRAVEL_BOX records are always matched with OMNI (no standalone TRAVEL_BOX exists)
        if has_omni_data:
            if start_date and end_date and start_date < end_date:
                delete_result = spark.sql(f"""
                    DELETE FROM {target_table}
                    WHERE (SALE_SOURCE_SYSTEM = 'OMNI' OR SALE_SOURCE_SYSTEM = 'TRAVEL_BOX')
                      AND (cast(TRANSACTION_TIMESTAMP as date) BETWEEN '{start_date}' AND '{end_date}'
                            OR TRANSACTION_TIMESTAMP is null)
                """)
            else:
                delete_result = spark.sql(f"""
                    DELETE FROM {target_table}
                    WHERE (SALE_SOURCE_SYSTEM = 'OMNI' OR SALE_SOURCE_SYSTEM = 'TRAVEL_BOX')
                      AND (cast(TRANSACTION_TIMESTAMP as date) >= '{timestamp}'
                            OR TRANSACTION_TIMESTAMP is null)
                """)
            deleted_count = delete_result.collect()[0]['num_affected_rows']
            print(f"Incremental Load")
        else:
            print("Full Load")

        # Ensure table exists with partitioning -- zero-cost (empty DataFrame, schema only)
        if not table_exists:
            try:
                #Create table with partitioning using DF schema
                (spark.createDataFrame([], df_transactions_omni.schema)
                    .write.format("delta")
                    .saveAsTable(target_table))
            except Exception:
                print("Table was created concurrently -- appending instead.")
                pass  # Table was created concurrently -- safe to continue

        # Single data write -- always exactly one append
        (df_transactions_omni
            .write.format("delta")
            .mode("append")
            .saveAsTable(target_table))

        # Get inserted row count from Delta table history -- avoids double computation
        history_metrics = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").select("operationMetrics").collect()[0][0]
        inserted_count = int(history_metrics.get("numOutputRows", 0))

        print("Transactions Omni Data Loaded")
        print(f"Rows Deleted: {deleted_count}")
        print(f"Rows Inserted: {inserted_count}")
        
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_transactions_omni.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,tbl readiness - update tag
readiness_table = f"transformation_{env}.plt_customer360.readiness_transactions_omni"

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
 
