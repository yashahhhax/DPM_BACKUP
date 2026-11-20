# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC **The `customer_kpi` table is the customer-level KPI data product consolidating metrics from Location Intelligence, Marketing and Transactions platinum sources.**
# MAGIC
# MAGIC 1. Source Tables:
# MAGIC
# MAGIC  Schema | Table | Purpose |
# MAGIC  --- | --- | --- |
# MAGIC  plt_customer360 | location_intelligence | Park visits, hotel stays, length of stay, churn, visits. customer_sk = entity_identifiers['scv_sk'] — business-aware SK |
# MAGIC  plt_customer360 | marketing | Email sent / opened / clicked KPIs. customer_sk = owner_scv_sk |
# MAGIC  plt_customer360 | transactions | Revenue KPIs. customer_sk = entity_identifiers['scv_sk'] — business-aware SK |
# MAGIC  plt_customer360 | master_kpi | KPI ID lookup |
# MAGIC
# MAGIC 2. Target Platinum table: **transformation_{env}.plt_customer360.customer_kpi**
# MAGIC
# MAGIC 3. Load Strategy: **Full Load (Run 1: TRUNCATE + INSERT) | Incremental (Run 2: SCD Type 1 MERGE — upsert on composite key)**
# MAGIC
# MAGIC 4. Table Design: **Narrow table — one row per customer per KPI per day**
# MAGIC
# MAGIC 5. Composite Merge Key: **report_date + report_hour + identifier_type + identifier_value + kpi_id**
# MAGIC
# MAGIC 6. REPORT_HOUR: **Always 0 for daily KPIs — column reserved for future hourly use**
# MAGIC
# MAGIC 7. Schema Enforcement implemented

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
# dbutils.widgets.text("env","dev")
# dbutils.widgets.text("current_job_run_id","")
# dbutils.widgets.text("identifier_type","omni_sk")   
# dbutils.widgets.text("report_date","")              
# env                = dbutils.widgets.get("env")
# current_job_run_id = dbutils.widgets.get("current_job_run_id")
# identifier_type    = dbutils.widgets.get("identifier_type")
# report_date        = dbutils.widgets.get("report_date")
# print(f"env                : {env}")
# print(f"current_job_run_id : {current_job_run_id}")
# print(f"identifier_type    : {identifier_type}")
# print(f"report_date        : {report_date}")

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

# DBTITLE 1,get notebook parameters
try:
    notebook_vars = ['env', 'current_job_run_id', 'identifier_type', 'report_date']
    env, current_job_run_id, identifier_type, report_date = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    msg = f"FAILED: [get_notebook_params] {str(e)}"
    print(msg)
    dbutils.notebook.exit(msg)

# COMMAND ----------

# DBTITLE 1,import dependencies
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, to_timestamp
from delta.tables import DeltaTable
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
from datetime import date as _date
import traceback

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

# DBTITLE 1,initialize source tables
report_date = report_date.strip() if report_date and report_date.strip() else str(_date.today() - timedelta(days=1))
print(f"report_date resolved : {report_date}")
print(f"identifier_type      : {identifier_type}")

tgt_db = f"transformation_{env}.plt_customer360"

src_loc_int  = f"{tgt_db}.location_intelligence"
src_mkt      = f"{tgt_db}.marketing"
src_txn      = f"{tgt_db}.transactions"
src_kpi_mst  = f"{tgt_db}.master_kpi"
target_table = f"{tgt_db}.customer_kpi"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")
print(f"Source table : {src_loc_int}")
print(f"Source table : {src_mkt}")
print(f"Source table : {src_txn}")
print(f"Source table : {src_kpi_mst}")
print(f"Target table : {target_table}")
print(f"UAE timestamp : {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,early exit check
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
if identifier_type in ["omni_sk"]:
    loc_categories = ["ATTENDANCE"]
else:
    loc_categories = ["ATTENDANCE", "HOTEL", "WIFI"]

loc_status = {cat: check_source_data(
    table_name=src_loc_int,
    date_column="CHECKIN_TIMESTAMP",
    report_date=report_date,
    additional_filter=f"""
        AND nullif(entity_identifiers['{identifier_type}'], '') IS NOT NULL
        AND CATEGORY = '{cat}'
    """
) for cat in loc_categories}

if identifier_type not in ["omni_sk"]:
    mkt_exists = check_source_data(
        table_name=src_mkt,
        date_column="event_time",
        report_date=report_date,
        additional_filter=f"AND nullif(entity_identifiers['{identifier_type}'], '') IS NOT NULL"
    )
else :
    mkt_exists = None

txn_exists = check_source_data(
    table_name=src_txn,
    date_column="SALE_TIMESTAMP",
    report_date=report_date,
    additional_filter=f"AND nullif(entity_identifiers['{identifier_type}'], '') IS NOT NULL"
)

#--------------------Print Status & Exit--------------------
for cat, status in loc_status.items():
    print(f"Location Intelligence - {cat:12} : {'Found' if status else 'No Data'}")
if identifier_type not in ["omni_sk"]:
    print(f"Marketing                            : {'Found' if mkt_exists else 'No Data'}")
print(f"Transactions                         : {'Found' if txn_exists else 'No Data'}")

missing_sources = [f"location_intelligence ({cat})" for cat, status in loc_status.items() if not status]
if not mkt_exists and identifier_type not in ["omni_sk"]: missing_sources.append("marketing")
if not txn_exists: missing_sources.append("transactions")

if missing_sources:
    raise Exception(
        f"No data found in: {', '.join(missing_sources)} "
        f"for report_date={report_date}."
    )

print(f"\nData found in all sources — continuing execution.")

# COMMAND ----------

# DBTITLE 1,load kpi master reference
spark.sql(f"""
    select kpi_id, kpi_name
    from {src_kpi_mst}
    where kpi_level   = 'customer'
      and active_flag = true
""").createOrReplaceTempView("vw_master_kpi")
print("vw_master_kpi tempview created.")

# COMMAND ----------

# DBTITLE 1,location intelligence KPI raw scan
# attendance: source (TUNNEL/annual_pass/fl_scv_sk), hotel: owner_scv_sk, wifi: owner_scv_sk
# vw_loc_intelligence_src reused in vw_location_kpis and visit inference — single scan
spark.sql(f"""
    select
        entity_identifiers['{identifier_type}']      as customer_sk
        ,CATEGORY
        ,CHECKIN_TIMESTAMP
        ,CHECKOUT_TIMESTAMP
        ,ASSET_NAME
        ,case
            -- stay duration in seconds, HOTEL only
            when CATEGORY = 'HOTEL'
             and CHECKOUT_TIMESTAMP is not null
             and CHECKIN_TIMESTAMP  is not null
            then unix_timestamp(CHECKOUT_TIMESTAMP) - unix_timestamp(CHECKIN_TIMESTAMP)
            else null
         end                                        as stay_seconds
    from {src_loc_int}
    where nullif(entity_identifiers['{identifier_type}'], '') is not null
      and CHECKIN_TIMESTAMP is not null
      and CATEGORY != 'FRICTIONLESS'
      and cast(CHECKIN_TIMESTAMP as date) <= '{report_date}'  -- exclude future bookings — matches Omni logic
      and (CHECKOUT_TIMESTAMP is null or cast(CHECKOUT_TIMESTAMP as date) <= '{report_date}')  -- completed stays only — matches Omni _calculate_visits() logic
""").createOrReplaceTempView("vw_loc_intelligence_src")


spark.sql(f"""
    with loc_window as (
        select
            customer_sk
            ,first_value(ASSET_NAME) over (partition by customer_sk order by CHECKIN_TIMESTAMP asc  rows between unbounded preceding and unbounded following) as first_park_visited
            ,first_value(ASSET_NAME) over (partition by customer_sk order by CHECKIN_TIMESTAMP desc rows between unbounded preceding and unbounded following) as latest_park_visited
        from vw_loc_intelligence_src
        where CATEGORY = 'ATTENDANCE'
    )
    ,loc_window_dedup as (
        select distinct customer_sk, first_park_visited, latest_park_visited
        from loc_window
    )
    ,favorit_park_base as (
        select
            customer_sk
            ,ASSET_NAME
            ,count(*)                as park_count
            ,max(CHECKIN_TIMESTAMP)  as latest_checkin
        from vw_loc_intelligence_src
        where CATEGORY = 'ATTENDANCE'
          and CHECKIN_TIMESTAMP >= add_months('{report_date}', -12)
        group by customer_sk, ASSET_NAME
    )
    ,favorit_park_ranked as (
        select customer_sk, ASSET_NAME as favorit_park
        from (
            select *
                   ,row_number() over (partition by customer_sk order by park_count desc, latest_checkin desc) as rn
            from favorit_park_base
        ) where rn = 1
    )
    ,loc_agg as (
        select
            customer_sk
            ,count(case when CATEGORY = 'ATTENDANCE' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -1)  then 1 end)  as no_park_visits_1mths
            ,count(case when CATEGORY = 'ATTENDANCE' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -3)  then 1 end)  as no_park_visits_3mths
            ,count(case when CATEGORY = 'ATTENDANCE' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -6)  then 1 end)  as no_park_visits_6mths
            ,count(case when CATEGORY = 'ATTENDANCE' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -12) then 1 end)  as no_park_visits_12mths
            ,count(case when CATEGORY = 'HOTEL' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -1)  then 1 end)       as no_hotel_stays_1mths
            ,count(case when CATEGORY = 'HOTEL' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -3)  then 1 end)       as no_hotel_stays_3mths
            ,count(case when CATEGORY = 'HOTEL' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -6)  then 1 end)       as no_hotel_stays_6mths
            ,count(case when CATEGORY = 'HOTEL' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -12) then 1 end)       as no_hotel_stays_12mths
            ,avg(case when CATEGORY = 'HOTEL' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -1)  then stay_seconds end)  as avg_length_of_stay_yas_in_seconds_1mths
            ,avg(case when CATEGORY = 'HOTEL' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -3)  then stay_seconds end)  as avg_length_of_stay_yas_in_seconds_3mths
            ,avg(case when CATEGORY = 'HOTEL' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -6)  then stay_seconds end)  as avg_length_of_stay_yas_in_seconds_6mths
            ,avg(case when CATEGORY = 'HOTEL' and CHECKIN_TIMESTAMP >= add_months('{report_date}', -12) then stay_seconds end)  as avg_length_of_stay_yas_in_seconds_12mths
            ,max(case when CATEGORY = 'ATTENDANCE' then CHECKIN_TIMESTAMP end)  as last_checkin_timestamp
            ,min(case when CATEGORY = 'ATTENDANCE' then CHECKIN_TIMESTAMP end)  as first_checkin_timestamp
        from vw_loc_intelligence_src
        group by customer_sk
    )
    select
        a.*
        ,w.first_park_visited
        ,w.latest_park_visited
        ,f.favorit_park
    from loc_agg a
    left join loc_window_dedup  w on a.customer_sk = w.customer_sk
    left join favorit_park_ranked f on a.customer_sk = f.customer_sk
""").createOrReplaceTempView("vw_location_kpis")

# COMMAND ----------

# DBTITLE 1,visit inference raw scan
# 5-day window visit session inference — UDF to Spark SQL
spark.sql(f"""
    WITH checkin_date AS (
        SELECT
            customer_sk,
            cast(CHECKIN_TIMESTAMP as date) AS CHECKIN_DATE,
            ASSET_NAME
        FROM vw_loc_intelligence_src
        WHERE CATEGORY IN ('ATTENDANCE', 'HOTEL')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY customer_sk, cast(CHECKIN_TIMESTAMP as date)
            ORDER BY CHECKIN_TIMESTAMP
        ) = 1
    ),
    next_check_in AS (
        SELECT *,
            COALESCE(
                LEAD(CHECKIN_DATE) OVER (PARTITION BY customer_sk ORDER BY CHECKIN_DATE),
                CHECKIN_DATE
            ) AS next_checkin
        FROM checkin_date
    ),
    filtered_check_in_list AS (
        SELECT *,
            filter(checkin_list, x -> datediff(x, CHECKIN_DATE) BETWEEN 1 AND 5) AS next_checkin_list
        FROM (
            SELECT *,
                array_distinct(sort_array(collect_set(next_checkin) OVER (PARTITION BY customer_sk))) AS checkin_list
            FROM next_check_in
        )
    ),
    check_in_explode AS (
        SELECT * FROM filtered_check_in_list
        LATERAL VIEW OUTER EXPLODE(next_checkin_list) t AS next_checked_in
    ),
    check_in_explode_distinct AS (
        SELECT *,
            CASE
                WHEN next_checked_in IS NULL THEN CHECKIN_DATE
                WHEN datediff(next_checked_in, CHECKIN_DATE) >= 5
                THEN COALESCE(
                    LAG(next_checked_in) OVER (PARTITION BY customer_sk, CHECKIN_DATE ORDER BY next_checked_in),
                    CHECKIN_DATE
                )
                ELSE next_checked_in
            END AS CHECKOUT_DATE
        FROM check_in_explode
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY customer_sk, CHECKIN_DATE ORDER BY next_checked_in DESC
        ) <= 2
    ),
    check_out AS (
        SELECT * FROM check_in_explode_distinct
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY customer_sk, CHECKIN_DATE ORDER BY CHECKOUT_DATE DESC
        ) = 1
    )
    SELECT customer_sk, CHECKIN_DATE, CHECKOUT_DATE, ASSET_NAME
    FROM check_out
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_sk, CHECKOUT_DATE ORDER BY CHECKIN_DATE
    ) = 1
""").createOrReplaceTempView("vw_visits_check_out")


spark.sql(f"""
    with visit_agg as (
        select
            customer_sk
            ,min(CHECKIN_DATE)                                                               as first_visit_date
            ,max(CHECKOUT_DATE)                                                              as last_visit_date
            ,count(case when CHECKIN_DATE >= add_months('{report_date}', -1)  then 1 end)    as total_number_visits_last_1_months
            ,count(case when CHECKIN_DATE >= add_months('{report_date}', -3)  then 1 end)    as total_number_visits_last_3_months
            ,count(case when CHECKIN_DATE >= add_months('{report_date}', -6)  then 1 end)    as total_number_visits_last_6_months
            ,count(case when CHECKIN_DATE >= add_months('{report_date}', -12) then 1 end)    as total_number_visits_last_12_months
        from vw_visits_check_out
        group by customer_sk
    )
    ,visit_window as (
        select
            customer_sk
            ,first_value(ASSET_NAME) over (
                partition by customer_sk
                order by CHECKOUT_DATE desc
                rows between unbounded preceding and unbounded following
            )                                    as last_visited_park
        from vw_visits_check_out
    )
    ,visit_window_dedup as (
        select distinct customer_sk, last_visited_park
        from visit_window
    )
    ,visit_dedup as (
        select a.customer_sk, a.first_visit_date, a.last_visit_date, w.last_visited_park
               ,a.total_number_visits_last_1_months
               ,a.total_number_visits_last_3_months
               ,a.total_number_visits_last_6_months
               ,a.total_number_visits_last_12_months
        from visit_agg a
        left join visit_window_dedup w on a.customer_sk = w.customer_sk
    )
    ,second_last as (
        select customer_sk, CHECKOUT_DATE as second_to_last_visit_date
        from (
            select customer_sk, CHECKOUT_DATE
                   ,row_number() over (partition by customer_sk order by CHECKOUT_DATE desc) as rn
            from vw_visits_check_out
        ) where rn = 2
    )
    select
        v.customer_sk
        ,v.first_visit_date
        ,v.last_visit_date
        ,v.last_visited_park
        ,s.second_to_last_visit_date
        ,v.total_number_visits_last_1_months
        ,v.total_number_visits_last_3_months
        ,v.total_number_visits_last_6_months
        ,v.total_number_visits_last_12_months
        ,datediff('{report_date}', v.last_visit_date)                as days_since_last_visit
        ,datediff(v.last_visit_date, s.second_to_last_visit_date)   as day_diff_last_2nd_last
        ,case
            when '{report_date}' = v.last_visit_date and v.first_visit_date = v.last_visit_date
                then 'newly_acquired_customer'
            when datediff('{report_date}', v.last_visit_date) = 90
                then 'churned'
            when '{report_date}' = v.last_visit_date
                 and v.first_visit_date != v.last_visit_date
                 and datediff(v.last_visit_date, s.second_to_last_visit_date) >= 90
                then 'returnee'
            when datediff('{report_date}', v.last_visit_date) < 90
                then 'active'
            else 'inactive'
        end                                                          as churn_status
        ,case
            when '{report_date}' = v.last_visit_date and v.first_visit_date = v.last_visit_date  then 1
            when datediff('{report_date}', v.last_visit_date) = 90                               then 0
            when '{report_date}' = v.last_visit_date
                 and datediff(v.last_visit_date, s.second_to_last_visit_date) >= 90             then 1
            when datediff('{report_date}', v.last_visit_date) < 90                              then 1
            else 0
        end                                                          as is_today_less_90_days
        ,case
            when '{report_date}' = v.last_visit_date and v.first_visit_date = v.last_visit_date  then null
            when datediff('{report_date}', v.last_visit_date) = 90                               then 1
            when '{report_date}' = v.last_visit_date
                 and datediff(v.last_visit_date, s.second_to_last_visit_date) >= 90             then 0
            when datediff('{report_date}', v.last_visit_date) < 90                              then 1
            else 0
        end                                                          as is_yesterday_less_90_days
    from visit_dedup v
    left join second_last s on v.customer_sk = s.customer_sk
""").createOrReplaceTempView("vw_visit_session_kpis")

# COMMAND ----------

# DBTITLE 1,email KPI raw scan
spark.sql(f"""
    with email_base as (
        select
            entity_identifiers['{identifier_type}']  as owner_scv_sk
            ,event_type
            ,event_time
            ,campaign_name
            ,campaign_type
            ,campaign_category
            ,campaign_launch_date
            ,others['emarsys_customer_id']          as customer_id
            ,campaign_key
        from {src_mkt}
        where nullif(entity_identifiers['{identifier_type}'], '') is not null
          and campaign_source = 'Emarsys-Email'
          and event_type      in ('email sends', 'email opens', 'email clicks')
    )
    ,email_agg as (
        select
            owner_scv_sk
            ,count(distinct case when event_type = 'email sends'  and campaign_launch_date >= add_months('{report_date}', -1)  then concat(customer_id,'-',campaign_key) end)  as total_email_sent_1mths
            ,count(distinct case when event_type = 'email sends'  and campaign_launch_date >= add_months('{report_date}', -3)  then concat(customer_id,'-',campaign_key) end)  as total_email_sent_3mths
            ,count(distinct case when event_type = 'email sends'  and campaign_launch_date >= add_months('{report_date}', -6)  then concat(customer_id,'-',campaign_key) end)  as total_email_sent_6mths
            ,count(distinct case when event_type = 'email sends'  and campaign_launch_date >= add_months('{report_date}', -12) then concat(customer_id,'-',campaign_key) end)  as total_email_sent_12mths
            ,count(distinct case when event_type = 'email opens'  and campaign_launch_date >= add_months('{report_date}', -1)  then concat(customer_id,'-',campaign_key) end)  as total_email_opened_1mths
            ,count(distinct case when event_type = 'email opens'  and campaign_launch_date >= add_months('{report_date}', -3)  then concat(customer_id,'-',campaign_key) end)  as total_email_opened_3mths
            ,count(distinct case when event_type = 'email opens'  and campaign_launch_date >= add_months('{report_date}', -6)  then concat(customer_id,'-',campaign_key) end)  as total_email_opened_6mths
            ,count(distinct case when event_type = 'email opens'  and campaign_launch_date >= add_months('{report_date}', -12) then concat(customer_id,'-',campaign_key) end)  as total_email_opened_12mths
            ,count(distinct case when event_type = 'email clicks' and campaign_launch_date >= add_months('{report_date}', -1)  then concat(customer_id,'-',campaign_key) end)  as total_email_clicked_1mths
            ,count(distinct case when event_type = 'email clicks' and campaign_launch_date >= add_months('{report_date}', -3)  then concat(customer_id,'-',campaign_key) end)  as total_email_clicked_3mths
            ,count(distinct case when event_type = 'email clicks' and campaign_launch_date >= add_months('{report_date}', -6)  then concat(customer_id,'-',campaign_key) end)  as total_email_clicked_6mths
            ,count(distinct case when event_type = 'email clicks' and campaign_launch_date >= add_months('{report_date}', -12) then concat(customer_id,'-',campaign_key) end)  as total_email_clicked_12mths
        from email_base
        group by owner_scv_sk
    )
    ,email_rates as (
        select *
            ,round(total_email_opened_1mths   / nullif(total_email_sent_1mths,   0), 4)  as email_engagement_open_rate_1mths
            ,round(total_email_clicked_1mths  / nullif(total_email_sent_1mths,   0), 4)  as email_engagement_click_rate_1mths
            ,round(total_email_opened_3mths   / nullif(total_email_sent_3mths,   0), 4)  as email_engagement_open_rate_3mths
            ,round(total_email_clicked_3mths  / nullif(total_email_sent_3mths,   0), 4)  as email_engagement_click_rate_3mths
            ,round(total_email_opened_6mths   / nullif(total_email_sent_6mths,   0), 4)  as email_engagement_open_rate_6mths
            ,round(total_email_clicked_6mths  / nullif(total_email_sent_6mths,   0), 4)  as email_engagement_click_rate_6mths
            ,round(total_email_opened_12mths  / nullif(total_email_sent_12mths,  0), 4)  as email_engagement_open_rate_12mths
            ,round(total_email_clicked_12mths / nullif(total_email_sent_12mths,  0), 4)  as email_engagement_click_rate_12mths
        from email_agg
    )
    ,email_response as (
        select
            owner_scv_sk
            ,avg(case when campaign_launch_date >= add_months('{report_date}', -1)  then open_response_hours  end) as email_open_avg_response_time_hours_1mths
            ,avg(case when campaign_launch_date >= add_months('{report_date}', -3)  then open_response_hours  end) as email_open_avg_response_time_hours_3mths
            ,avg(case when campaign_launch_date >= add_months('{report_date}', -6)  then open_response_hours  end) as email_open_avg_response_time_hours_6mths
            ,avg(case when campaign_launch_date >= add_months('{report_date}', -12) then open_response_hours  end) as email_open_avg_response_time_hours_12mths
            ,avg(case when campaign_launch_date >= add_months('{report_date}', -1)  then click_response_hours end) as email_click_avg_response_time_hours_1mths
            ,avg(case when campaign_launch_date >= add_months('{report_date}', -3)  then click_response_hours end) as email_click_avg_response_time_hours_3mths
            ,avg(case when campaign_launch_date >= add_months('{report_date}', -6)  then click_response_hours end) as email_click_avg_response_time_hours_6mths
            ,avg(case when campaign_launch_date >= add_months('{report_date}', -12) then click_response_hours end) as email_click_avg_response_time_hours_12mths
        from (
            select
                owner_scv_sk
                ,campaign_launch_date
                ,concat(customer_id,'-',campaign_key)                                                              as unique_email_key
                ,(min(case when event_type = 'email opens'  then unix_timestamp(event_time) end)
                - min(case when event_type = 'email sends'  then unix_timestamp(event_time) end)) / 3600.0         as open_response_hours
                ,(min(case when event_type = 'email clicks' then unix_timestamp(event_time) end)
                - min(case when event_type = 'email sends'  then unix_timestamp(event_time) end)) / 3600.0         as click_response_hours
            from email_base
            group by owner_scv_sk, campaign_launch_date, concat(customer_id,'-',campaign_key)
        )
        group by owner_scv_sk
    )
    ,last_opened as (
        select owner_scv_sk
               ,campaign_name          as last_opened_email_campaign_name
               ,campaign_type          as last_opened_email_campaign_type
               ,campaign_category      as last_opened_email_campaign_category
               ,campaign_launch_date   as last_opened_email_campaign_launch_date
               ,event_time             as last_opened_email_time
        from (
            select *
                   ,row_number() over (partition by owner_scv_sk order by event_time desc) as rn
            from email_base
            where event_type = 'email opens'
        ) where rn = 1
    )
    ,last_clicked as (
        select owner_scv_sk
               ,campaign_name          as last_clicked_email_campaign_name
               ,campaign_type          as last_clicked_email_campaign_type
               ,campaign_category      as last_clicked_email_campaign_category
               ,campaign_launch_date   as last_clicked_email_campaign_launch_date
               ,event_time             as last_clicked_email_time
        from (
            select *
                   ,row_number() over (partition by owner_scv_sk order by event_time desc) as rn
            from email_base
            where event_type = 'email clicks'
        ) where rn = 1
    )
    select
        r.*
        ,res.email_open_avg_response_time_hours_1mths
        ,res.email_open_avg_response_time_hours_3mths
        ,res.email_open_avg_response_time_hours_6mths
        ,res.email_open_avg_response_time_hours_12mths
        ,res.email_click_avg_response_time_hours_1mths
        ,res.email_click_avg_response_time_hours_3mths
        ,res.email_click_avg_response_time_hours_6mths
        ,res.email_click_avg_response_time_hours_12mths
        ,lo.last_opened_email_campaign_name
        ,lo.last_opened_email_campaign_type
        ,lo.last_opened_email_campaign_category
        ,lo.last_opened_email_campaign_launch_date
        ,lo.last_opened_email_time
        ,lc.last_clicked_email_campaign_name
        ,lc.last_clicked_email_campaign_type
        ,lc.last_clicked_email_campaign_category
        ,lc.last_clicked_email_campaign_launch_date
        ,lc.last_clicked_email_time
    from email_rates r
    left join email_response res using (owner_scv_sk)
    left join last_opened   lo  using (owner_scv_sk)
    left join last_clicked  lc  using (owner_scv_sk)
""").createOrReplaceTempView("vw_email_kpi")

# COMMAND ----------

# DBTITLE 1,transaction KPI raw scan
spark.sql(f"""
    select
        entity_identifiers['{identifier_type}']                                                                                                                                                                                        as customer_sk
        ,sum(case when SALE_TIMESTAMP >= add_months('{report_date}', -1)  then NET_SALE_AMOUNT_NO_TAX else 0 end)                                                                                                                    as total_revenue_generated_last_1_months
        ,sum(case when SALE_TIMESTAMP >= add_months('{report_date}', -3)  then NET_SALE_AMOUNT_NO_TAX else 0 end)                                                                                                                    as total_revenue_generated_last_3_months
        ,sum(case when SALE_TIMESTAMP >= add_months('{report_date}', -6)  then NET_SALE_AMOUNT_NO_TAX else 0 end)                                                                                                                    as total_revenue_generated_last_6_months
        ,sum(case when SALE_TIMESTAMP >= add_months('{report_date}', -12) then NET_SALE_AMOUNT_NO_TAX else 0 end)                                                                                                                    as total_revenue_generated_last_12_months
    from {src_txn}
    where nullif(entity_identifiers['{identifier_type}'], '') is not null
    group by entity_identifiers['{identifier_type}']
""").createOrReplaceTempView("vw_transaction_kpis")

# COMMAND ----------

# DBTITLE 1,customer kpi wide transformation
spark.sql(f"""
    with vw_scv_base as (
        select customer_sk from (
            select distinct customer_sk
            from vw_loc_intelligence_src
            union all
            select distinct owner_scv_sk as customer_sk
            from vw_email_kpi
            where owner_scv_sk is not null  
            union all
            select distinct customer_sk
            from vw_transaction_kpis
        )
        group by customer_sk
    )
    select
        scv.customer_sk
        -- COALESCE with 0 for count KPIs — non-visitors show 0 not NULL
        ,cast(coalesce(loc.no_park_visits_1mths,  0)                                    as string)  as NO_PARK_VISITS_1MTHS
        ,cast(coalesce(loc.no_hotel_stays_1mths,  0)                                    as string)  as NO_HOTEL_STAYS_1MTHS
        ,cast(loc.avg_length_of_stay_yas_in_seconds_1mths                               as string)  as AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_1MTHS
        ,cast(coalesce(loc.no_park_visits_3mths,  0)                                    as string)  as NO_PARK_VISITS_3MTHS
        ,cast(coalesce(loc.no_hotel_stays_3mths,  0)                                    as string)  as NO_HOTEL_STAYS_3MTHS
        ,cast(loc.avg_length_of_stay_yas_in_seconds_3mths                               as string)  as AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_3MTHS
        ,cast(coalesce(loc.no_park_visits_6mths,  0)                                    as string)  as NO_PARK_VISITS_6MTHS
        ,cast(coalesce(loc.no_hotel_stays_6mths,  0)                                    as string)  as NO_HOTEL_STAYS_6MTHS
        ,cast(loc.avg_length_of_stay_yas_in_seconds_6mths                               as string)  as AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_6MTHS
        ,cast(coalesce(loc.no_park_visits_12mths, 0)                                    as string)  as NO_PARK_VISITS_12MTHS
        ,cast(coalesce(loc.no_hotel_stays_12mths, 0)                                    as string)  as NO_HOTEL_STAYS_12MTHS
        ,cast(loc.avg_length_of_stay_yas_in_seconds_12mths                              as string)  as AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_12MTHS
        ,cast(loc.last_checkin_timestamp                                        as string)  as LAST_CHECKIN_TIMESTAMP
        ,cast(loc.latest_park_visited                                           as string)  as LATEST_PARK_VISITED
        ,cast(loc.first_checkin_timestamp                                       as string)  as FIRST_CHECKIN_TIMESTAMP
        ,cast(loc.first_park_visited                                            as string)  as FIRST_PARK_VISITED
        ,cast(loc.favorit_park                                                  as string)  as FAVORIT_PARK
        ,cast(eml.total_email_sent_1mths                                        as string)  as TOTAL_EMAIL_SENT_1MTHS
        ,cast(eml.total_email_opened_1mths                                      as string)  as TOTAL_EMAIL_OPENED_1MTHS
        ,cast(eml.total_email_clicked_1mths                                     as string)  as TOTAL_EMAIL_CLICKED_1MTHS
        ,cast(eml.email_engagement_open_rate_1mths                              as string)  as EMAIL_ENGAGEMENT_OPEN_RATE_1MTHS
        ,cast(eml.email_engagement_click_rate_1mths                             as string)  as EMAIL_ENGAGEMENT_CLICK_RATE_1MTHS
        ,cast(eml.email_open_avg_response_time_hours_1mths                      as string)  as EMAIL_OPEN_AVG_RESPONSE_TIME_HOURS_1MTHS
        ,cast(eml.email_click_avg_response_time_hours_1mths                     as string)  as EMAIL_CLICK_AVG_RESPONSE_TIME_HOURS_1MTHS
        ,cast(eml.total_email_sent_3mths                                        as string)  as TOTAL_EMAIL_SENT_3MTHS
        ,cast(eml.total_email_opened_3mths                                      as string)  as TOTAL_EMAIL_OPENED_3MTHS
        ,cast(eml.total_email_clicked_3mths                                     as string)  as TOTAL_EMAIL_CLICKED_3MTHS
        ,cast(eml.email_engagement_open_rate_3mths                              as string)  as EMAIL_ENGAGEMENT_OPEN_RATE_3MTHS
        ,cast(eml.email_engagement_click_rate_3mths                             as string)  as EMAIL_ENGAGEMENT_CLICK_RATE_3MTHS
        ,cast(eml.email_open_avg_response_time_hours_3mths                      as string)  as EMAIL_OPEN_AVG_RESPONSE_TIME_HOURS_3MTHS
        ,cast(eml.email_click_avg_response_time_hours_3mths                     as string)  as EMAIL_CLICK_AVG_RESPONSE_TIME_HOURS_3MTHS
        ,cast(eml.total_email_sent_6mths                                        as string)  as TOTAL_EMAIL_SENT_6MTHS
        ,cast(eml.total_email_opened_6mths                                      as string)  as TOTAL_EMAIL_OPENED_6MTHS
        ,cast(eml.total_email_clicked_6mths                                     as string)  as TOTAL_EMAIL_CLICKED_6MTHS
        ,cast(eml.email_engagement_open_rate_6mths                              as string)  as EMAIL_ENGAGEMENT_OPEN_RATE_6MTHS
        ,cast(eml.email_engagement_click_rate_6mths                             as string)  as EMAIL_ENGAGEMENT_CLICK_RATE_6MTHS
        ,cast(eml.email_open_avg_response_time_hours_6mths                      as string)  as EMAIL_OPEN_AVG_RESPONSE_TIME_HOURS_6MTHS
        ,cast(eml.email_click_avg_response_time_hours_6mths                     as string)  as EMAIL_CLICK_AVG_RESPONSE_TIME_HOURS_6MTHS
        ,cast(eml.total_email_sent_12mths                                       as string)  as TOTAL_EMAIL_SENT_12MTHS
        ,cast(eml.total_email_opened_12mths                                     as string)  as TOTAL_EMAIL_OPENED_12MTHS
        ,cast(eml.total_email_clicked_12mths                                    as string)  as TOTAL_EMAIL_CLICKED_12MTHS
        ,cast(eml.email_engagement_open_rate_12mths                             as string)  as EMAIL_ENGAGEMENT_OPEN_RATE_12MTHS
        ,cast(eml.email_engagement_click_rate_12mths                            as string)  as EMAIL_ENGAGEMENT_CLICK_RATE_12MTHS
        ,cast(eml.email_open_avg_response_time_hours_12mths                     as string)  as EMAIL_OPEN_AVG_RESPONSE_TIME_HOURS_12MTHS
        ,cast(eml.email_click_avg_response_time_hours_12mths                    as string)  as EMAIL_CLICK_AVG_RESPONSE_TIME_HOURS_12MTHS
        ,cast(eml.last_opened_email_campaign_name                               as string)  as LAST_OPENED_EMAIL_CAMPAIGN_NAME
        ,cast(eml.last_opened_email_campaign_type                               as string)  as LAST_OPENED_EMAIL_CAMPAIGN_TYPE
        ,cast(eml.last_opened_email_campaign_category                           as string)  as LAST_OPENED_EMAIL_CAMPAIGN_CATEGORY
        ,cast(eml.last_opened_email_campaign_launch_date                        as string)  as LAST_OPENED_EMAIL_CAMPAIGN_LAUNCH_DATE
        ,cast(eml.last_opened_email_time                                        as string)  as LAST_OPENED_EMAIL_TIME
        ,cast(eml.last_clicked_email_campaign_name                              as string)  as LAST_CLICKED_EMAIL_CAMPAIGN_NAME
        ,cast(eml.last_clicked_email_campaign_type                              as string)  as LAST_CLICKED_EMAIL_CAMPAIGN_TYPE
        ,cast(eml.last_clicked_email_campaign_category                          as string)  as LAST_CLICKED_EMAIL_CAMPAIGN_CATEGORY
        ,cast(eml.last_clicked_email_campaign_launch_date                       as string)  as LAST_CLICKED_EMAIL_CAMPAIGN_LAUNCH_DATE
        ,cast(eml.last_clicked_email_time                                       as string)  as LAST_CLICKED_EMAIL_TIME
        ,cast(txn.total_revenue_generated_last_1_months                         as string)  as TOTAL_REVENUE_GENERATED_LAST_1_MONTHS
        ,cast(vis.total_number_visits_last_1_months                             as string)  as TOTAL_NUMBER_VISITS_LAST_1_MONTHS
        -- AVG_SPENT = revenue / visit sessions (not SALE_ID count) 
        ,cast(round(txn.total_revenue_generated_last_1_months  / nullif(vis.total_number_visits_last_1_months,  0), 2) as string)  as AVG_SPENT_VISIT_LAST_1_MONTHS
        ,cast(txn.total_revenue_generated_last_3_months                         as string)  as TOTAL_REVENUE_GENERATED_LAST_3_MONTHS
        ,cast(vis.total_number_visits_last_3_months                             as string)  as TOTAL_NUMBER_VISITS_LAST_3_MONTHS
        ,cast(round(txn.total_revenue_generated_last_3_months  / nullif(vis.total_number_visits_last_3_months,  0), 2) as string)  as AVG_SPENT_VISIT_LAST_3_MONTHS
        ,cast(txn.total_revenue_generated_last_6_months                         as string)  as TOTAL_REVENUE_GENERATED_LAST_6_MONTHS
        ,cast(vis.total_number_visits_last_6_months                             as string)  as TOTAL_NUMBER_VISITS_LAST_6_MONTHS
        ,cast(round(txn.total_revenue_generated_last_6_months  / nullif(vis.total_number_visits_last_6_months,  0), 2) as string)  as AVG_SPENT_VISIT_LAST_6_MONTHS
        ,cast(txn.total_revenue_generated_last_12_months                        as string)  as TOTAL_REVENUE_GENERATED_LAST_12_MONTHS
        ,cast(vis.total_number_visits_last_12_months                            as string)  as TOTAL_NUMBER_VISITS_LAST_12_MONTHS
        ,cast(round(txn.total_revenue_generated_last_12_months / nullif(vis.total_number_visits_last_12_months, 0), 2) as string)  as AVG_SPENT_VISIT_LAST_12_MONTHS
        ,cast(vis.churn_status                                                  as string)  as CHURN_STATUS
        ,cast(vis.days_since_last_visit                                         as string)  as DAYS_SINCE_LAST_VISIT
        ,cast(vis.is_today_less_90_days                                         as string)  as IS_TODAY_LESS_90_DAYS
        ,cast(vis.is_yesterday_less_90_days                                     as string)  as IS_YESTERDAY_LESS_90_DAYS
    from vw_scv_base                          scv
    left join vw_location_kpis   loc on scv.customer_sk = loc.customer_sk
    left join vw_email_kpi                   eml on scv.customer_sk = eml.owner_scv_sk
    left join vw_transaction_kpis             txn on scv.customer_sk = txn.customer_sk
    left join vw_visit_session_kpis           vis on scv.customer_sk = vis.customer_sk
""").createOrReplaceTempView("vw_customer_kpi_pivoted")

# COMMAND ----------

# DBTITLE 1,customer kpi unpivot transformation
kpi_columns = [c for c in spark.table("vw_customer_kpi_pivoted").columns if c != "customer_sk"]
stack_expr = ", ".join([f"'{c}', `{c}`" for c in kpi_columns])

spark.sql(f"""
    select
        cast('{report_date}' as date) as report_date
        ,0                  as report_hour
        ,'{identifier_type}' as identifier_type
        ,customer_sk             as identifier_value
        ,kpi_name
        ,kpi_value
    from vw_customer_kpi_pivoted
    lateral view stack({len(kpi_columns)}, {stack_expr}) t as kpi_name, kpi_value
""").createOrReplaceTempView("vw_customer_kpi_unpivoted")

# COMMAND ----------

# DBTITLE 1,customer kpi join with master kpi
df_c360_customer_kpis = spark.sql("""
    select
        n.report_date
        ,n.report_hour
        ,n.identifier_type
        ,n.identifier_value
        ,m.kpi_id
        ,n.kpi_value
    from vw_customer_kpi_unpivoted n
    inner join vw_master_kpi m on upper(n.kpi_name) = upper(m.kpi_name)
    where n.kpi_value is not null
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Schema casting

# COMMAND ----------

# DBTITLE 1,schema casting
df_c360_customer_kpis = apply_schema_casting(df_c360_customer_kpis, customer_kpi_schema)

# COMMAND ----------

# DBTITLE 1,write to UC
customer_kpi_ddl = ",\n".join(
    f"{col_name} {dtype}" for col_name, dtype in customer_kpi_schema.items()
)

try:
    ddl_struct = StructType.fromDDL(customer_kpi_ddl)
    df_c360_customer_kpis = df_c360_customer_kpis \
        .withColumn("created_at",  to_timestamp(lit(current_uae_timestamp))) \
        .withColumn("modified_at", to_timestamp(lit(current_uae_timestamp)))

    if compare_schema_lenient(df_c360_customer_kpis.schema, ddl_struct):
        print("Schema matches!")

        # Check if table exists
        inserted_count = 0
        updated_count  = 0
        table_exists = spark.catalog.tableExists(target_table)

        # Get watermark
        if table_exists:
            watermark = spark.sql(f"SELECT MAX(report_date) FROM {target_table} WHERE identifier_type = '{identifier_type}'").collect()[0][0]
        else:
            watermark = None

        # Load mode
        is_full_load = watermark is None

        # Full load — DELETE + INSERT
        if is_full_load:
            print(f"Load mode  : FULL LOAD")
            if table_exists:
                spark.sql(f"DELETE FROM {target_table} WHERE identifier_type = '{identifier_type}'")
            df_c360_customer_kpis.write.format("delta").mode("append").saveAsTable(target_table)
            inserted_count = spark.sql(f"SELECT COUNT(*) FROM {target_table} WHERE identifier_type = '{identifier_type}'").collect()[0][0]
            updated_count  = 0
            #--------------------OPTIMIZE + ZORDER — compact files and co-locate data for faster downstream reads--------------------
            spark.sql(f"OPTIMIZE {target_table} ZORDER BY (identifier_type, report_date, identifier_value)")
        else:
            # Incremental — SCD Type 1 MERGE on composite key, created_at preserved
            print(f"Load mode  : Incremental (report_date: {report_date})")
            delta_tgt = DeltaTable.forName(spark, target_table)
            df_result = (delta_tgt.alias("tgt")
                .merge(df_c360_customer_kpis.alias("src"),
                       "tgt.report_date = src.report_date AND tgt.report_hour = src.report_hour AND "
                       "tgt.identifier_type = src.identifier_type AND tgt.identifier_value = src.identifier_value AND "
                       "tgt.kpi_id = src.kpi_id")
                .whenMatchedUpdate(set={
                    "kpi_value"   : "src.kpi_value",
                    "modified_at" : "src.modified_at"
                })
                .whenNotMatchedInsertAll()
                .execute())
            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

        #--------------------7-day retention — runs for both full load and incremental--------------------
        spark.sql(f"DELETE FROM {target_table} WHERE identifier_type = '{identifier_type}' AND report_date < date_sub('{report_date}', 30)")

        print(f"Customer KPI Table Loaded...")
        print(f"Rows Inserted : {inserted_count}")
        print(f"Rows Updated  : {updated_count}")
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_c360_customer_kpis.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    msg = f"FAILED: [write to UC] {str(e)}"
    print(msg)
    dbutils.notebook.exit(msg)

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

print("Customer KPI notebook completed successfully.")
dbutils.notebook.exit("SUCCESS")
