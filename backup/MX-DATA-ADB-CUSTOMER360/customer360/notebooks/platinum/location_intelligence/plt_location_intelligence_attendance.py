# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC This notebook processes sales and attendance data, applies schema enforcement, and loads results into the `location_intelligence` table using an incremental strategy.
# MAGIC 1. Source Gold Table: 
# MAGIC | Schema | Table |
# MAGIC | --- | --- |
# MAGIC |gld_travelbox |dwh_vw_sales |
# MAGIC | gld_drr |tbvw_attendance|
# MAGIC slv_facepassreporting | slv_lkp_frictionless_data_tickets_snapshot|
# MAGIC slv_facepassreporting | slv_lkp_frictionless_data_account_snapshot|
# MAGIC
# MAGIC 2. Target Platinum table: **location_intelligence**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 
# MAGIC
# MAGIC 4. Load Strategy: **Incremental Load (18 months)**
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

# DBTITLE 1,include view ddl
# MAGIC %run ../../../config/view_queries

# COMMAND ----------

# DBTITLE 1,include table config
# MAGIC %run ../../../config/table_config

# COMMAND ----------

# # Create widgets for notebook parameters
# dbutils.widgets.text("env", "dev")
# dbutils.widgets.text("start_date", "")
# dbutils.widgets.text("end_date", "")
# dbutils.widgets.text("current_job_run_id", "")
# dbutils.widgets.text("buffer_days", "7")

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
attendance  = f"transformation_{env}.gld_drr.tbvw_attendance"
company = f"transformation_{env}.gld_shared.dwh_dim_company"
data_ticket_table = f"transformation_{env}.slv_facepassreporting.slv_lkp_frictionless_data_tickets_snapshot"

#target table
target_table = f"transformation_{env}.plt_customer360.location_intelligence"
#master tables
scv_account_master = f"transformation_{env}.plt_customer360.scv_account_master"
omni_account_master = f"transformation_{env}.plt_customer360.omni_account_master"


current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")


print(f"Source table: {attendance}")
print(f"Source tables: {vw_sales}")
print(f"Facepass tables: {data_ticket_table}")
print(f"Target table: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,filter condition
if (not spark.catalog.tableExists(target_table)) or (spark.sql(f"SELECT 1 FROM {target_table} WHERE CATEGORY = 'ATTENDANCE' LIMIT 1").count() == 0):
    print("Table not exists or ATTENDANCE data not present, Full Load")
    attendance_filter = "1=1"

elif start_date and end_date and start_date < end_date: 
    print("Table exists, Incremental Load with date range")
    attendance_filter = f"cast(a.`Attendance Date Time` as date) BETWEEN '{start_date}' AND '{end_date}'"
else:
    print("Table exists, Incremental Load")
    #apply buffer days
    timestamp = (datetime.now(ZoneInfo("Asia/Dubai")) - timedelta(days=buffer_days)).strftime('%Y-%m-%d')
    #set attendance filter
    attendance_filter = f"cast(a.`Attendance Date Time` as date) >= '{timestamp}'"

print(f"Attendance filter : {attendance_filter}")


# COMMAND ----------

# DBTITLE 1,Hotel tickets
df_location_intelligence_hotel_tickets = spark.sql(f"""
      select 
      `Ticket ID` AS TICKET_ID
      ,`Booking No` AS BOOKING_NO
      ,`Product Name` AS PRODUCT_NAME
      ,`Email address` AS EMAIL_ADDRESS
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
            and `Theme Park Flag` = 'Y'
            and `Ticket ID` is not null
            and `Booking No` is not null
qualify (row_number() over (partition by `Ticket ID`,`Booking No` order by `Booking Creation Date` desc)) = 1
""")

df_location_intelligence_hotel_tickets.createOrReplaceTempView("hotel_tickets")

# COMMAND ----------

# DBTITLE 1,Theam Park Attendance
df_location_inteligence_attendance_raw = spark.sql(f"""
WITH attendance AS (
    SELECT
    a.`YAS ID` AS YAS_ID
    ,`Email` AS EMAIL_ADDRESS
    ,a.`EH Booking ID` AS BOOKING_NO
    ,a.`Attendance Date Time` AS ATTENDENCE_DATETIME
    ,a.`Base Ticket ID` AS BASE_TICKET_ID
    ,a.`Ticket Id` AS TICKET_ID
    ,a.`Redemption Company Name` AS REDEMPTION_COMPANY_NAME
    ,a.`Gate Siteid` AS GATE_SITE_ID
    ,a.`Source Name` AS SOURCE_NAME
    ,a.`Product Name` AS PRODUCT_NAME
    ,a.`AP MYPASS ID` AS AP_MYPASS_ID
    ,a.`AP ACCOUNT ID` AS AP_ACCOUNT_ID
    ,a.`OMNI Account ID` AS OMNI_ACCOUNT_ID
    ,CASE WHEN a.`Annual Pass Flag` = 'Not Available' THEN NULL
          WHEN a.`Redemption Company Name` not in ('Ferrari World Abu Dhabi','SeaWorld Abu Dhabi','Warner Bros','Yas Waterworld') THEN 'N'
          ELSE a.`Annual Pass Flag` 
    END AS ANNUAL_PASS_FLAG
    ,a.`Media Code` AS MEDIA_CODE
    FROM {attendance} a 
    where {attendance_filter}
    AND a.`Source Name` <> 'SHAREPOINT'
    -- deduplication
    QUALIFY ROW_NUMBER() OVER(PARTITION BY  a.`Ticket Id`, a.`Source Name`, IFNULL(a.`AP ACCOUNT ID`, a.`OMNI Account ID`),a.`Redemption Company Name`, date(a.`Attendance Date Time`), hour(a.`Attendance Date Time`) ORDER BY a.`Attendance Date Time` asc) = 1
),fp_base as (
    select
    frictionless_data_tickets.mediaidentifier
    ,{yas_id_standardization("frictionless_data_tickets.mypassid")} as mypassid
    from {data_ticket_table} frictionless_data_tickets 
    qualify( row_number() over (partition by frictionless_data_tickets.mediaidentifier order by frictionless_data_tickets.snapdate desc, frictionless_data_tickets.active asc,frictionless_data_tickets.updated desc)) = 1
),parent_scv as(
    select 
    email_id,scv_account_sk 
    from {scv_account_master}
    where source_name = 't_mdm_guest'
    QUALIFY row_number() over(partition by email_id order by modified_date desc) = 1
),attendance_join as (
    SELECT
    a.MEDIA_CODE
    ,a.OMNI_ACCOUNT_ID
    ,a.AP_ACCOUNT_ID
    ,{yas_id_standardization("a.AP_MYPASS_ID")} as AP_MYPASS_ID
    ,{email_standardization("a.EMAIL_ADDRESS")} AS EMAIL_ADDRESS
    ,{email_standardization("t.EMAIL_ADDRESS")} AS tb_EMAIL_ADDRESS
    ,{yas_id_standardization("a.YAS_ID")} AS YAS_ID --//if tb email is null, then att.yas_id else null
    ,"ATTENDANCE" as CATEGORY
    ,a.ATTENDENCE_DATETIME AS CHECKIN_TIMESTAMP
    ,a.ANNUAL_PASS_FLAG
    ,coalesce(t.PRODUCT_NAME, a.PRODUCT_NAME)  AS PRODUCT_NAME
    ,a.BASE_TICKET_ID
    ,a.TICKET_ID
    ,a.BOOKING_NO
    ,a.REDEMPTION_COMPANY_NAME
    ,a.GATE_SITE_ID AS GATE_ID
    ,a.SOURCE_NAME
    ,iff(a.AP_ACCOUNT_ID != a.OMNI_ACCOUNT_ID and lower(trim(a.AP_MYPASS_ID)) = lower(trim(a.YAS_ID)) ,0,1) as valid_ap_yas_id_flag
  from attendance a
  left join hotel_tickets t on
    a.TICKET_ID = t.TICKET_ID AND a.BOOKING_NO = t.BOOKING_NO
)
select
  IFF(scv_tb.scv_account_sk is not null, scv_tb.scv_account_sk, scv.scv_account_sk) as purchaser_scv_sk
  ,IFF(scv_tb.scv_account_sk is not null, NULL,omni.omni_account_sk) as purchaser_omni_sk
  ,IFF(scv_tb.scv_account_sk is not null, scv_tb.scv_account_sk, scv2.scv_account_sk) as owner_scv_sk
  ,IFF(scv_tb.scv_account_sk is not null, NULL, omni2.omni_account_sk ) as owner_omni_sk
  ,scv_fp.scv_account_sk as fl_scv_sk
  ,map('scv_sk', IFF(attendance.SOURCE_NAME = 'TUNNEL',purchaser_scv_sk,  IFF(attendance.ANNUAL_PASS_FLAG = 'Y', owner_scv_sk, coalesce(fl_scv_sk,owner_scv_sk))),
     'omni_sk', IFF(attendance.SOURCE_NAME = 'TUNNEL', purchaser_omni_sk, IFF(attendance.ANNUAL_PASS_FLAG = 'Y',owner_omni_sk, IFF(fl_scv_sk is not null,NULL,owner_omni_sk))),
     'email_id',IFF(attendance.SOURCE_NAME = 'TUNNEL',attendance.EMAIL_ADDRESS,  IFF(attendance.ANNUAL_PASS_FLAG = 'Y', omni2.email_id , coalesce(attendance.tb_EMAIL_ADDRESS,scv_fp.email_id, omni2.email_id, scv2.email_id)  ))
     ) as entity_identifiers
  ,attendance.CATEGORY
  ,attendance.REDEMPTION_COMPANY_NAME as ASSET_NAME
  ,attendance.CHECKIN_TIMESTAMP
  ,NULL as CHECKOUT_TIMESTAMP
  ,IFF( attendance.SOURCE_NAME = 'TUNNEL','N',IFNULL(attendance.ANNUAL_PASS_FLAG,'N')) as ANNUAL_PASS_FLAG
  ,nullif(trim(attendance.PRODUCT_NAME), '') as PRODUCT_NAME
  ,attendance.SOURCE_NAME
  ,map(
      'email_id',attendance.EMAIL_ADDRESS,
      'yas_id',attendance.YAS_ID,
      'ap_mypass_id',attendance.AP_MYPASS_ID,
      'facepass_mypass_id',string(fp.mypassid),
      'omni_account_id',string(attendance.OMNI_ACCOUNT_ID),
      'ap_account_id',string(attendance.AP_ACCOUNT_ID),
      'base_ticket_id',string(attendance.BASE_TICKET_ID),
      'ticket_id', string(attendance.TICKET_ID),
      'media_code', string(attendance.MEDIA_CODE),
      'travelbox_booking_no',string(attendance.BOOKING_NO),
      'travelbox_email_id',attendance.tb_EMAIL_ADDRESS,
      'gate_id', string(attendance.GATE_ID),
      'entry_type',NULL
    ) as OTHERS
from attendance_join attendance
left join fp_base fp on fp.mediaidentifier::string = attendance.MEDIA_CODE::string
left join parent_scv scv_tb on  scv_tb.email_id = attendance.tb_EMAIL_ADDRESS
left join {scv_account_master} scv_fp on scv_fp.yas_id = fp.mypassid and scv_fp.active_yas_id_flag = 'Y'
left join {omni_account_master} omni on omni.account_id = attendance.OMNI_ACCOUNT_ID
left join {omni_account_master} omni2 on omni2.account_id = attendance.AP_ACCOUNT_ID 
left join {scv_account_master} scv on  scv.yas_id = attendance.YAS_ID 
                                    and scv.email_id = omni.email_id 
                                    and (scv.dob = omni.dob or scv.dob is null or omni.dob is null)
                                    and scv.active_yas_id_flag = 'Y'
left join {scv_account_master} scv2 on scv2.yas_id = attendance.AP_MYPASS_ID 
                                    and attendance.valid_ap_yas_id_flag = 1 
                                    and scv2.email_id = omni2.email_id 
                                    and (scv2.dob = omni2.dob or scv2.dob is null or omni2.dob is null)
                                    and scv2.active_yas_id_flag = 'Y'
-- removed null checkin timestamp records 
where attendance.CHECKIN_TIMESTAMP is not null
  """)

df_location_inteligence_attendance_raw = map_asset_category(df_location_inteligence_attendance_raw,"ASSET_NAME")

df_location_inteligence_attendance_raw.createOrReplaceTempView("attendance_raw")                       

# COMMAND ----------

# DBTITLE 1,fianl attendance

df_location_inteligence_attendance = spark.sql(f"""
select 
  a.entity_identifiers
  ,a.purchaser_scv_sk
  ,a.purchaser_omni_sk
  ,a.owner_scv_sk
  ,a.owner_omni_sk 
  ,a.fl_scv_sk 
  ,a.category
  ,a.asset_name
  ,a.checkin_timestamp
  ,a.checkout_timestamp
  ,a.annual_pass_flag
  ,a.product_name
  ,case when a.purchaser_scv_sk is null and
            a.purchaser_omni_sk is null and
            a.owner_scv_sk is null and
            a.fl_scv_sk is null and
            a.owner_omni_sk is null then 'N'
            else 'Y' end as valid_sk  -- all sk are null then N else Y
  ,a.SOURCE_NAME as source_name
  ,'{current_uae_timestamp}' as etl_created_datetime
  ,'{current_uae_timestamp}' as etl_updated_datetime
  ,a.others
  from attendance_raw a
  """)

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_location_inteligence_attendance = apply_schema_casting(df_location_inteligence_attendance,location_inteligence_schema )

# COMMAND ----------

# DBTITLE 1,Write to UC
# Build DDL string from schema dict
location_inteligence_dll = ",\n".join(
    f"{col} {dtype}" for col, dtype in location_inteligence_schema.items()
)
 
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(location_inteligence_dll)
    if compare_schema_lenient(df_location_inteligence_attendance.schema, ddl_struct):
        print("Schema matches!")

        deleted_count = 0
        inserted_count = 0

        # Check table state
        table_exists = spark.catalog.tableExists(target_table)
        has_attendance_data = table_exists and spark.sql(f"SELECT 1 FROM {target_table} WHERE CATEGORY = 'ATTENDANCE' LIMIT 1").count() > 0

        if has_attendance_data:
            if start_date and end_date and start_date < end_date:
                delete_result = spark.sql(f"""
                    DELETE FROM {target_table}
                    WHERE cast(CHECKIN_TIMESTAMP as date) BETWEEN '{start_date}' AND '{end_date}'
                    AND CATEGORY = 'ATTENDANCE'
                """)
                deleted_count = delete_result.collect()[0]['num_affected_rows']
            else:
                delete_result = spark.sql(f"""
                    DELETE FROM {target_table}
                    WHERE cast(CHECKIN_TIMESTAMP as date) >= '{timestamp}'
                    AND CATEGORY = 'ATTENDANCE'
                """)
                deleted_count = delete_result.collect()[0]['num_affected_rows']
            print("Incremental Load")
        else:
            print("Full Load")

        # Ensure table exists with partitioning — zero-cost (empty DataFrame, schema only)
        if not table_exists:
            try:
                (spark.createDataFrame([], df_location_inteligence_attendance.schema)
                    .write.format("delta")
                    .saveAsTable(target_table))
            except Exception:
                print("Table was created concurrently -- appending instead.")
                pass

        # Single data write — always exactly one append
        (df_location_inteligence_attendance
            .write.format("delta")
            .mode("append")
            .saveAsTable(target_table))

        
        # Get inserted row count from Delta table history — avoids double computation
        history_metrics = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").select("operationMetrics").collect()[0][0]
        inserted_count = int(history_metrics.get("numOutputRows", 0))

        print("Location Intlligence Attendance Data Loaded")
        print(f"Rows Deleted: {deleted_count}")
        print(f"Rows Inserted: {inserted_count}")
        
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_location_inteligence_attendance.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,tbl readiness - update tag
readiness_table = f"transformation_{env}.plt_customer360.readiness_location_intelligence_attendance"

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
    "fact_updated_records_count": 0
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)
 
