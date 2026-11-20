# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## This notebooks populates KPI's for marketing, location_intelligence and transactions on asset level for last 1,3,6 and 12 months
# MAGIC
# MAGIC ##  Table Summary
# MAGIC
# MAGIC |Source Tables|use|
# MAGIC |------|-----|
# MAGIC |plt_customer360.marketing|for email asset kpi's|
# MAGIC |plt_customer360.location_intelligence|total and avg visit|
# MAGIC |plt_customer360.transactions|total and avg revenue|
# MAGIC
# MAGIC |Target Table|
# MAGIC |---|
# MAGIC |plt_customer360.asset_kpi|
# MAGIC
# MAGIC ### Schema enforcement implemented
# MAGIC
# MAGIC ### **Load Strategy:** Delete and load (keeping history of previous report dates)

# COMMAND ----------

# DBTITLE 1,import_dependency
from pyspark.sql.functions import col, when, current_timestamp, trim, lower, lit, regexp_replace,regexp_extract, instr, split, row_number, to_date, date_format, upper,min,max,expr,coalesce,length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
import traceback

# COMMAND ----------

# DBTITLE 1,schema_config
# MAGIC %run ../../../config/schema_config

# COMMAND ----------

# DBTITLE 1,base_functions
# MAGIC %run ../../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,view_functions
# %run ../../../config/view_queries

# COMMAND ----------

# DBTITLE 1,get notebook params
try:
    notebook_vars = [
        'env','report_date','current_job_run_id'
    ]
    env,report_date,current_job_run_id = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,global error handler
# ── Global error handler — catches ANY unhandled exception in any cell ──
def _notebook_error_handler(self, etype, evalue, tb, tb_offset=None):
    if "Exit" in etype.__name__ or "SystemExit" in etype.__name__:
        return
    error_msg = f"FAILED: [{etype.__name__}] {str(evalue)}"
    print(error_msg)
    dbutils.notebook.exit(error_msg)

if current_job_run_id:
    get_ipython().set_custom_exc((Exception,), _notebook_error_handler)

# COMMAND ----------

# DBTITLE 1,schema_table_name
# schema
plt_schema = f"transformation_{env}.plt_customer360"

# src tables
marketing_table = f"{plt_schema}.marketing"
location_intelligence_table = f"{plt_schema}.location_intelligence"
transaction_table = f"{plt_schema}.transactions"
kpi_master_table  = f"{plt_schema}.master_kpi"

# target tables

target_table = f"{plt_schema}.asset_kpi"

# current timestamp
current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")
current_uae_date = datetime.now(ZoneInfo("Asia/Dubai")).date()

print(f"Marketing Table: {marketing_table}")
print(f"Location Intelligence Table: {location_intelligence_table}")
print(f"Transactions Table: {transaction_table}")
print(f"Kpi Master Table: {kpi_master_table}")
print(f"Target Table: {target_table}")

# COMMAND ----------

# DBTITLE 1,filter_condition
kpi_date = report_date if report_date.strip() else (current_uae_date - timedelta(days=1)).strftime("%Y-%m-%d")

filter_condition_marketing = (f"event_time <= '{kpi_date}'")
filter_condition_loc_intelligence = (f"checkin_timestamp <= '{kpi_date}'")
filter_condition_transaction = (f"sale_timestamp <= '{kpi_date}'")

print(f"Filter Condition: {filter_condition_marketing}\n{filter_condition_loc_intelligence}\n{filter_condition_transaction}")

# COMMAND ----------

# DBTITLE 1,source_data_validation
#--------------------Standard Source Validation Logic--------------------
def check_source_data(table_name, date_column, report_date, additional_filter=""):
    query = f"""
        SELECT 1 FROM {table_name}
        WHERE CAST({date_column} AS DATE) >= '{report_date}' 
        {additional_filter}
        LIMIT 1
    """
    return bool(spark.sql(query).head(1))

#--------------------Source Checks--------------------
loc_categories = ["ATTENDANCE", "HOTEL", "WIFI"]
loc_status = {cat: check_source_data(
    table_name=location_intelligence_table,
    date_column="CHECKIN_TIMESTAMP",
    report_date=kpi_date,
    additional_filter=f"""
        AND CATEGORY = '{cat}'
        AND nullif(entity_identifiers['email_id'], '') IS NOT NULL
    """
) for cat in loc_categories}

mkt_exists = check_source_data(
    table_name=marketing_table,
    date_column="event_time",
    report_date=kpi_date
)

txn_exists = check_source_data(
    table_name=transaction_table,
    date_column="SALE_TIMESTAMP",
    report_date=kpi_date,
    additional_filter=f"""
        AND nullif(entity_identifiers['email_id'], '') IS NOT NULL
    """
)

#--------------------Print Status & Exit--------------------
for cat, status in loc_status.items():
    print(f"Location Intelligence - {cat:12} : {'Found' if status else 'No Data'}")
print(f"Marketing                            : {'Found' if mkt_exists else 'No Data'}")
print(f"Transactions                         : {'Found' if txn_exists else 'No Data'}")

missing_sources = [f"location_intelligence ({cat})" for cat, status in loc_status.items() if not status]
if not mkt_exists: missing_sources.append("marketing")
if not txn_exists: missing_sources.append("transactions")

if missing_sources:
    raise Exception(f"\nNo data found in: {', '.join(missing_sources)} for report_date={kpi_date} — exiting notebook.")

print(f"\nData found in all sources — continuing execution.")

# COMMAND ----------

# DBTITLE 1,marketing_src_table

spark.sql(f"""
        select campaign_name,
        campaign_type,
        campaign_category,
        campaign_launch_date,
        entity_identifiers.email_id as customer_sk,
        event_type,
        event_time,
        concat_ws('-',others['emarsys_customer_id'],campaign_key) as unique_email_key,
        account_name
        from {marketing_table} 
        where 1=1 and {filter_condition_marketing}
        and campaign_source = 'Emarsys-Email'
        and event_type in ('email sends','email opens','email clicks')
        """).createOrReplaceTempView("vw_marketing_src")


# COMMAND ----------

# DBTITLE 1,email_monthly_rates
spark.sql(f"""
        
with email_level AS(
SELECT
    account_name,
    unique_email_key,

    -- 1-month counts ---
    count(distinct CASE WHEN Event_Type = 'email sends'  AND Campaign_Launch_Date > add_months('{kpi_date}',-1) THEN unique_email_key END) AS sent_1m,
    count(distinct CASE WHEN Event_Type = 'email opens'  AND Campaign_Launch_Date > add_months('{kpi_date}',-1) THEN unique_email_key END) AS opened_1m,
    count(distinct CASE WHEN Event_Type = 'email clicks' AND Campaign_Launch_Date > add_months('{kpi_date}',-1) THEN unique_email_key END) AS clicked_1m,

    -- 3-month count ---
    count(distinct CASE WHEN Event_Type = 'email sends'  AND Campaign_Launch_Date > add_months('{kpi_date}',-3) THEN unique_email_key END) AS sent_3m,
    count(distinct CASE WHEN Event_Type = 'email opens'  AND Campaign_Launch_Date > add_months('{kpi_date}',-3) THEN unique_email_key END) AS opened_3m,
    count(distinct CASE WHEN Event_Type = 'email clicks' AND Campaign_Launch_Date > add_months('{kpi_date}',-3) THEN unique_email_key END) AS clicked_3m,

    -- 6-month count ----
    count(distinct CASE WHEN Event_Type = 'email sends'  AND Campaign_Launch_Date > add_months('{kpi_date}',-6) THEN unique_email_key END) AS sent_6m,
    count(distinct CASE WHEN Event_Type = 'email opens'  AND Campaign_Launch_Date > add_months('{kpi_date}',-6) THEN unique_email_key END) AS opened_6m,
    count(distinct CASE WHEN Event_Type = 'email clicks' AND Campaign_Launch_Date > add_months('{kpi_date}',-6) THEN unique_email_key END) AS clicked_6m,

    --12-month count -----
    count(distinct CASE WHEN Event_Type = 'email sends'  AND Campaign_Launch_Date > add_months('{kpi_date}',-12) THEN unique_email_key END) AS sent_12m,
    count(distinct CASE WHEN Event_Type = 'email opens'  AND Campaign_Launch_Date > add_months('{kpi_date}',-12) THEN unique_email_key END) AS opened_12m,
    count(distinct CASE WHEN Event_Type = 'email clicks' AND Campaign_Launch_Date > add_months('{kpi_date}',-12) THEN unique_email_key END) AS clicked_12m

FROM vw_marketing_src
GROUP BY account_name, unique_email_key
),
email_rates as(
    select account_name,
    ------------- 1 months-----------------
    COALESCE(SUM(sent_1m),    0)  AS total_email_sent_1mths,
    COALESCE(SUM(opened_1m),  0)  AS total_email_opened_1mths,
    COALESCE(SUM(clicked_1m), 0)  AS total_email_clicked_1mths,
    SUM(opened_1m)  / nullif(SUM(sent_1m),0) AS email_engagement_open_rate_1mths,
    SUM(clicked_1m) / nullif(SUM(opened_1m),0) AS email_engagement_click_rate_1mths,
    ------------- 3 months-----------------
    COALESCE(SUM(sent_3m),    0)  AS total_email_sent_3mths,
    COALESCE(SUM(opened_3m),  0)  AS total_email_opened_3mths,
    COALESCE(SUM(clicked_3m), 0)  AS total_email_clicked_3mths,
    SUM(opened_3m)  / nullif(SUM(sent_3m),0) AS email_engagement_open_rate_3mths,
    SUM(clicked_3m) / nullif(SUM(opened_3m),0) AS email_engagement_click_rate_3mths,
    ------------- 6 months-----------------
    COALESCE(SUM(sent_6m),    0) AS total_email_sent_6mths,
    COALESCE(SUM(opened_6m),  0) AS total_email_opened_6mths,
    COALESCE(SUM(clicked_6m), 0) AS total_email_clicked_6mths,
    SUM(opened_6m)  / nullif(SUM(sent_6m),0) AS email_engagement_open_rate_6mths,
    SUM(clicked_6m) / nullif(SUM(opened_6m),0) AS email_engagement_click_rate_6mths,
    ------------- 12 months-----------------
    COALESCE(SUM(sent_12m),    0) AS total_email_sent_12mths,
    COALESCE(SUM(opened_12m),  0) AS total_email_opened_12mths,
    COALESCE(SUM(clicked_12m), 0) AS total_email_clicked_12mths,
    SUM(opened_12m)  / nullif(SUM(sent_12m),0) AS email_engagement_open_rate_12mths,
    SUM(clicked_12m) / nullif(SUM(opened_12m),0) AS email_engagement_click_rate_12mths
    from email_level
    group by account_name
),
email_avg_response as(
    select account_name,
    avg(CASE WHEN Campaign_Launch_Date > add_months('{kpi_date}',-1) THEN open_time_hours END) AS email_open_avg_response_time_hours_1mths,
    avg(CASE WHEN Campaign_Launch_Date > add_months('{kpi_date}',-1) THEN click_time_hours END) AS email_click_avg_response_time_hours_1mths,
    avg(CASE WHEN Campaign_Launch_Date > add_months('{kpi_date}',-3) THEN open_time_hours  END) AS email_open_avg_response_time_hours_3mths,
    avg(CASE WHEN Campaign_Launch_Date > add_months('{kpi_date}',-3) THEN click_time_hours END) AS email_click_avg_response_time_hours_3mths,
    avg(CASE WHEN Campaign_Launch_Date > add_months('{kpi_date}',-6) THEN open_time_hours  END) AS email_open_avg_response_time_hours_6mths,
    avg(CASE WHEN Campaign_Launch_Date > add_months('{kpi_date}',-6) THEN click_time_hours END) AS email_click_avg_response_time_hours_6mths,
    avg(CASE WHEN Campaign_Launch_Date > add_months('{kpi_date}',-12) THEN open_time_hours  END) AS email_open_avg_response_time_hours_12mths,
    avg(CASE WHEN Campaign_Launch_Date > add_months('{kpi_date}',-12) THEN click_time_hours END) AS email_click_avg_response_time_hours_12mths
    from(
    SELECT
    account_name,unique_email_key,Campaign_Launch_Date,
    (UNIX_TIMESTAMP(MIN(CASE WHEN Event_Type = 'email opens'  THEN Event_Time END)) - UNIX_TIMESTAMP(MIN(CASE WHEN Event_Type = 'email sends'  THEN Event_Time END)))  / 3600.0 AS open_time_hours,
    (UNIX_TIMESTAMP(MIN(CASE WHEN Event_Type = 'email clicks' THEN Event_Time END)) - UNIX_TIMESTAMP(MIN(CASE WHEN Event_Type = 'email sends'  THEN Event_Time END))) / 3600.0 AS click_time_hours
    FROM vw_marketing_src
    group by account_name,unique_email_key,Campaign_Launch_Date
    )
    group by account_name
)

SELECT *
FROM email_rates er left join email_avg_response evr using (account_name)

""").createOrReplaceTempView("vw_email_monthly_agg")

# COMMAND ----------

# DBTITLE 1,email_campaign_kpi
spark.sql(f"""
select 
account_name,
max(case when event_type = 'email opens' then Campaign_Name end) as LAST_OPENED_EMAIL_CAMPAIGN_NAME,
max(case when event_type = 'email opens' then Campaign_Type end) as LAST_OPENED_EMAIL_CAMPAIGN_TYPE,
max(case when event_type = 'email opens' then Campaign_Category end) as LAST_OPENED_EMAIL_CAMPAIGN_CATEGORY,
max(case when event_type = 'email opens' then Campaign_Launch_Date end) as LAST_OPENED_EMAIL_CAMPAIGN_LAUNCH_DATE,
max(case when event_type = 'email opens' then Event_Time end) as LAST_OPENED_EMAIL_TIME,
max(case when event_type = 'email clicks' then Campaign_Name end) as LAST_CLICKED_EMAIL_CAMPAIGN_NAME,
max(case when event_type = 'email clicks' then Campaign_Type end) as LAST_CLICKED_EMAIL_CAMPAIGN_TYPE,
max(case when event_type = 'email clicks' then Campaign_Category end) as LAST_CLICKED_EMAIL_CAMPAIGN_CATEGORY,
max(case when event_type = 'email clicks' then Campaign_Launch_Date end) as LAST_CLICKED_EMAIL_CAMPAIGN_LAUNCH_DATE,
max(case when event_type = 'email clicks' then Event_Time end) as LAST_CLICKED_EMAIL_TIME
from 
(
    select account_name, event_type, Campaign_Name, Campaign_Type, Campaign_Category, Campaign_Launch_Date, Event_Time
    from vw_marketing_src
    where event_type in ('email opens','email clicks')
    qualify(row_number() over(partition by account_name, event_type order by event_time desc)) = 1
)
group by account_name
""").createOrReplaceTempView("vw_email_campaign_agg")

# COMMAND ----------

# DBTITLE 1,final_email_kpi
df_email_kpi = spark.sql(f"""
    select * from vw_email_monthly_agg emg full outer join vw_email_campaign_agg ecg 
    using (account_name)
""").createOrReplaceTempView("vw_email_kpi")

# COMMAND ----------

# DBTITLE 1,udf_alternative_sql
df_check_out = spark.sql(f"""
    -- Converted timestamp to date in src_selection and remove nulls
    with loc_intelligence_base as(
      select 
        entity_identifiers.email_id as customer_sk,
        asset_name,
        CHECKIN_TIMESTAMP,
        cast(CHECKIN_TIMESTAMP as date) AS CHECKIN_DATE,
        category
        from {location_intelligence_table} 
        where 1=1 and entity_identifiers.email_id IS NOT NULL 
        AND CHECKIN_TIMESTAMP IS NOT NULL and {filter_condition_loc_intelligence}
    ),
    checkin_date AS(
    SELECT
      *
    FROM loc_intelligence_base
    -- Deduplicate — keep only first occurrence per EMAIL_ADDRESS + CHECKIN_DATE
    QUALIFY(row_number() over(PARTITION BY customer_sk ,CHECKIN_DATE ORDER BY CHECKIN_TIMESTAMP)) = 1
    ),
    next_check_in as (
      SELECT *,coalesce(LEAD(CHECKIN_DATE,1) OVER (PARTITION BY customer_sk  ORDER BY CHECKIN_TIMESTAMP),CHECKIN_DATE) AS next_checkin FROM checkin_date 
    )
    ,filtered_check_in_list as(
    select * ,filter(checkin_list,x-> (datediff(x,CHECKIN_DATE) between 1 and 5)) as next_checkin_list 
    from (
      select *,sort_array(collect_set(next_checkin) over(partition by customer_sk )) as checkin_list 
      from next_check_in
      )
    )
    ,
    check_in_explode as (
    select * from filtered_check_in_list
    LATERAL VIEW OUTER EXPLODE(next_checkin_list) t AS next_checked_in 
    )
    ,
    check_in_explode_distinct as(
      SELECT *,
      case 
        when next_checked_in is null then CHECKIN_DATE 
        when (datediff(next_checked_in,CHECKIN_DATE)) >= 5 then coalesce(lag(next_checked_in,1) over(partition by customer_sk ,checkin_date order by next_checked_in),CHECKIN_DATE)
        else next_checked_in 
      end as checkout_date 
      FROM check_in_explode
      -- last two records if days_diff >= 5
      qualify (row_number() over (partition by customer_sk ,checkin_date order by next_checked_in desc))<=2
    --   order by checkin_date,next_checked_in
    )
    ,
    check_out as (
    select *
    from check_in_explode_distinct
    -- keep prev date record in case of days_diff >= 5
    qualify row_number() over (partition by customer_sk ,checkin_date order by checkout_date desc) = 1
    )
    select customer_sk,asset_name,checkin_date,checkout_date,category from check_out
    -- check_out dedup and keep min check_in date
    qualify row_number() over (partition by customer_sk,checkout_date order by checkin_date) = 1

""").createOrReplaceTempView("vw_visits_check_out")

# COMMAND ----------

# DBTITLE 1,total_visits
df_total_visits = spark.sql(f"""
    select ASSET_NAME,
    count(case when CHECKIN_DATE >= add_months('{kpi_date}',-1) then 1 end) as TOTAL_NUMBER_VISITS_LAST_1_MONTHS,
    count(case when CHECKIN_DATE >= add_months('{kpi_date}',-3) then 1 end) as TOTAL_NUMBER_VISITS_LAST_3_MONTHS,
    count(case when CHECKIN_DATE >= add_months('{kpi_date}',-6) then 1 end) as TOTAL_NUMBER_VISITS_LAST_6_MONTHS,  
    count(case when CHECKIN_DATE >= add_months('{kpi_date}',-12) then 1 end) as TOTAL_NUMBER_VISITS_LAST_12_MONTHS
    from vw_visits_check_out
    group by ASSET_NAME   
""").createOrReplaceTempView("vw_total_visits")

# COMMAND ----------

# DBTITLE 1,transactions_kpi
df_transactions_agg = spark.sql(f"""
with transaction_base as (
select entity_identifiers.email_id as customer_sk,
sale_id,
ticket_id,
SALE_TIMESTAMP,
SELLING_ENTITY as ASSET_NAME,
NET_SALE_AMOUNT_NO_TAX
from {transaction_table} 
where 1=1 and {filter_condition_transaction}
and entity_identifiers.email_id is not null
)
select ASSET_NAME,
sum(case when SALE_TIMESTAMP >= dateadd(add_months('{kpi_date}',-1),-10) then NET_SALE_AMOUNT_NO_TAX end) as TOTAL_REVENUE_GENERATED_LAST_1_MONTHS,
sum(case when SALE_TIMESTAMP >= dateadd(add_months('{kpi_date}',-3),-10) then NET_SALE_AMOUNT_NO_TAX end) as TOTAL_REVENUE_GENERATED_LAST_3_MONTHS,
sum(case when SALE_TIMESTAMP >= dateadd(add_months('{kpi_date}',-6),-10) then NET_SALE_AMOUNT_NO_TAX end) as TOTAL_REVENUE_GENERATED_LAST_6_MONTHS,
sum(case when SALE_TIMESTAMP >= dateadd(add_months('{kpi_date}',-12),-10) then NET_SALE_AMOUNT_NO_TAX end) as TOTAL_REVENUE_GENERATED_LAST_12_MONTHS
from transaction_base
group by ASSET_NAME
""").createOrReplaceTempView("vw_transactions_agg")

# COMMAND ----------

# DBTITLE 1,transaction+loc_intelligence
df_marketing_kpis = spark.sql(f"""
select *,
(t.TOTAL_REVENUE_GENERATED_LAST_1_MONTHS/nullif(v.TOTAL_NUMBER_VISITS_LAST_1_MONTHS,0)) as AVG_SPENT_VISIT_LAST_1_MONTHS,
(t.TOTAL_REVENUE_GENERATED_LAST_3_MONTHS/nullif(v.TOTAL_NUMBER_VISITS_LAST_3_MONTHS,0)) as AVG_SPENT_VISIT_LAST_3_MONTHS,
(t.TOTAL_REVENUE_GENERATED_LAST_6_MONTHS/nullif(v.TOTAL_NUMBER_VISITS_LAST_6_MONTHS,0)) as AVG_SPENT_VISIT_LAST_6_MONTHS,
(t.TOTAL_REVENUE_GENERATED_LAST_12_MONTHS/nullif(v.TOTAL_NUMBER_VISITS_LAST_12_MONTHS,0)) as AVG_SPENT_VISIT_LAST_12_MONTHS
from vw_transactions_agg t
full outer join vw_total_visits v using (ASSET_NAME)

""").createOrReplaceTempView("vw_visit_revenue_kpis")

# COMMAND ----------

# DBTITLE 1,email+marketing/visit
df_asset_kpi = spark.sql(f"""
select coalesce(e.account_name,m.asset_name) as asset_name,e.* except (e.account_name),m.* except (m.asset_name) from vw_email_kpi e full outer join vw_visit_revenue_kpis m on e.account_name = m.ASSET_NAME
""")

df_asset_kpi.createOrReplaceTempView("Asset_kpi_agg")

# COMMAND ----------

# DBTITLE 1,asset_kpi_unpivot
kpi_columns = [col for col in df_asset_kpi.columns if col not in ['asset_name']]
kpi_stack = ", ".join([f"'{col}', CAST({col} AS STRING)" for col in kpi_columns])

spark.sql(f"""
    SELECT
    cast('{kpi_date}' as date) as report_date,
    0 as report_hour,
    asset_name,
    kpi_name,
    kpi_value
    FROM asset_kpi_agg
    LATERAL VIEW stack({len(kpi_columns)},{kpi_stack}) AS kpi_name,kpi_value
    WHERE asset_name is not null and lower(asset_name) not in ("annual pass")
""").createOrReplaceTempView("asset_kpi_unpivot")


# COMMAND ----------

# DBTITLE 1,Final_asset_kpi_table
df_asset_kpi = spark.sql(f"""
    select report_date,
    report_hour,
    asset_name,   --company code
    m.kpi_id,
    kpi_value,
    ('{current_uae_timestamp}') as etl_created_datetime,
    ('{current_uae_timestamp}') as etl_updated_datetime
    from asset_kpi_unpivot a 
    inner join {kpi_master_table} m on lower(a.kpi_name) = lower(m.kpi_name) and lower(m.kpi_level) = 'asset'
    where a.kpi_value is not null
""")

# COMMAND ----------

# DBTITLE 1,schema casting
df_asset_kpi = apply_schema_casting(df_asset_kpi,asset_kpi_schema)

# COMMAND ----------

# DBTITLE 1,writing to uc

# Build DDL string from schema dict

asset_kpi_ddl = ",\n".join(
    f"`{col}` {dtype}" for col, dtype in asset_kpi_schema.items()
)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(asset_kpi_ddl)
    if compare_schema_lenient(df_asset_kpi.schema, ddl_struct):
        print("Schema matches!")

        #delete and insert based on report_date
        if spark.catalog.tableExists(target_table):
            spark.sql(f"delete from {target_table} where report_date = '{kpi_date}'")
        
        (df_asset_kpi.write.format("delta")
                        .mode("append")
                        .saveAsTable(target_table))
        print("inserting done")
    # Extract counts
        metrics = spark.sql(f"DESCRIBE HISTORY {target_table}").first()["operationMetrics"]
        deleted_rows = metrics.get("numDeletedRows", 0)
        inserted_rows = metrics.get("numOutputRows", 0)

        print(f"deleted {deleted_rows} rows for '{kpi_date}'")
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_asset_kpi.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    error_msg = f"FAILED: [write to UC] {str(e)}"
    print(error_msg)
    dbutils.notebook.exit(error_msg)

# COMMAND ----------

# DBTITLE 1,update_latest_tag
# add last_updated tag with current timestamp to identify table last refreshed datetime.
add_last_updated_tag(target_table)

# COMMAND ----------

# DBTITLE 1,plt_logging
# Prepare platinum table statistics dictionary for logging

plt_stats = {
    "gld_table": target_table,
    "dim_new_records_count": 0,
    "dim_updated_records_count": 0,
    "fact_inserted_records_count": inserted_rows,
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
trigger_databricks_job_by_name('workflow_orchestration')

print("Customer KPI notebook completed successfully.")
dbutils.notebook.exit("SUCCESS")
