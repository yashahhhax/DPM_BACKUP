# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC
# MAGIC ### Pipeline Overview
# MAGIC
# MAGIC | Attribute        | Details                                            |
# MAGIC | ---------------- | -------------------------------------------------- |
# MAGIC | **Table Name**   | customer_survey                                    |
# MAGIC | **Layer**        | Platinum                                           |
# MAGIC | **Target Table** | transformation_dev.plt_customer360.customer_survey |
# MAGIC | **Domain**       | Customer 360 – Customer Survey Analytics           |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Source Details
# MAGIC
# MAGIC | Schema | Table              | Purpose                              |
# MAGIC | ------ | ------------------ | ------------------------------------ |
# MAGIC | gld    | vw_survey_analysis | Source – customer survey response data |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Load Strategy
# MAGIC
# MAGIC | Load Type            | Description                                                                                     |
# MAGIC | -------------------- | ----------------------------------------------------------------------------------------------- |
# MAGIC | **Full Load**        | Initial run (TRUNCATE + INSERT)                                                                 |
# MAGIC | **Incremental Load** | MERGE on response_id using response_date with 7-day backward buffer to handle late-arriving data |
# MAGIC | **Date Range Load**  | Filtered MERGE on response_date when start_date and end_date are provided                       |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Technical Details
# MAGIC
# MAGIC | Attribute              | Details                    |
# MAGIC | ---------------------- | -------------------------- |
# MAGIC | **Primary Key**        | response_id                |
# MAGIC | **Business Key(s)**    | customer_id                |
# MAGIC | **Watermark Column**   | response_date              |
# MAGIC | **Buffer Days**        | 7                          |
# MAGIC | **Schema Enforcement** | Yes (pre-load validation)  |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Notes
# MAGIC
# MAGIC | Item               | Description                          |
# MAGIC | ------------------ | ------------------------------------ |
# MAGIC | Late Arriving Data | Handled using 7-day backward buffer  |
# MAGIC | Idempotency        | Ensured via MERGE logic              |
# MAGIC | Data Freshness     | Controlled via response_date watermark |
# MAGIC | Schema Validation  | Performed before load execution      |
# MAGIC | Merge Strategy     | whenMatchedUpdateAll / whenNotMatchedInsertAll |

# COMMAND ----------

from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import List
import pandas as pd
import numpy as np
import pyspark.sql as SQL
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, col, add_months, current_date, expr, trim, from_json
from pyspark.sql.types import StructType, StringType, MapType
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Schema_config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,base_function
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,view_queries
# MAGIC %run ../../config/view_queries

# COMMAND ----------

# dbutils.widgets.text("env", "dev", "Environment")
# dbutils.widgets.text("start_date", "", "Start Date")
# dbutils.widgets.text("end_date", "", "End Date")
# dbutils.widgets.text("current_job_run_id", "", "Current Job Run ID")
# dbutils.widgets.text("buffer_days", "7", "Buffer Days")

# COMMAND ----------

# DBTITLE 1,notebook_vars
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

# DBTITLE 1,Table, Schema
schema = f"transformation_{env}.gld_gss"
tgt_db = f"transformation_{env}.plt_customer360"

# Source table names
vw_survey_agg_analysis = f"{schema}.vw_survey_response_flat"
src_scv   = f"{tgt_db}.scv_account_master"

# Target table name
target_table = f"transformation_{env}.plt_customer360.customer_survey"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table: {vw_survey_agg_analysis}")
print(f"Target table: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")

# COMMAND ----------

df_final_customer_survey = spark.sql(f"""
WITH

-- SCV Dedup
scv_active as (
    select email_id, scv_account_sk
    from (
        select email_id, scv_account_sk,
               ROW_NUMBER() OVER (
                   PARTITION BY email_id
                   ORDER BY modified_date DESC
               ) as rn
        from {src_scv}
        where source_name = 't_mdm_guest'
          and email_id IS NOT NULL
    ) sub
    where rn = 1
),

-- ALL PARK DATA
all_park_data AS (
SELECT
    survey.response_id,
    survey.visit_date,
    survey.response_date,
    survey.facility_name,
    {email_standardization("survey.email_address")} as email_address,
    survey.survey_name,

    map(
        'email_id', nullif(survey.email_address,'Not Available'),
        'nationality', survey.SURV_DIM_NATIONALITY,
        'cor', survey.SURV_DIM_COR,
        'emirate', survey.SURV_DIM_EMIRATE,
        'gender', survey.SURV_DIM_GENDER,
        'age_grp', survey.SURV_DIM_AGE_GRP
    ) as respondent_details,

    map(
        'park_entry_type', survey.SURV_DIM_PARK_ENTRY_TYPE,
        'facepass_enrollment_mode', survey.SURV_DIM_FACEPASS_ENROLLMENT_MODE,
        'no_of_visits', survey.SURV_DIM_NO_OF_VISITS,
        'ticket_type', survey.SURV_DIM_TICKET_TYPE,
        'ap_purchase_reason', survey.surv_dim_ap_purchase_reason,
        'ap_non_purchase_reason', survey.surv_dim_ap_non_purchase_reason
    ) as entry_details,

    survey.SURV_SCORE_NPS as nps_score,
    survey.SURV_SCORE_GSS as gss_score,

    map(
        'ease_of_reaching', CAST(survey.SURV_SCORE_EASE_OF_REACHING AS STRING),
        'speed_of_entry', CAST(survey.SURV_SCORE_SPEED_OF_ENTRY AS STRING),
        'overall_digital_enrollment_experience', CAST(survey.SURV_SCORE_OVERALL_DIGITAL_ENROLLMENT_EXPERIENCE AS STRING),
        'food_and_beverage', CAST(survey.SURV_SCORE_FOOD_AND_BEVERAGE AS STRING),
        'shopping', CAST(survey.SURV_SCORE_SHOPPING AS STRING),
        'arrival_experience', CAST(survey.SURV_SCORE_ARRIVAL_EXPERIENCE AS STRING),
        'rides_and_attractions', CAST(survey.SURV_SCORE_RIDES_AND_ATTRACTIONS AS STRING),
        'live_shows_and_characters', CAST(survey.SURV_SCORE_LIVE_SHOWS_AND_CHARACTERS AS STRING),
        'park_cleanliness', CAST(survey.SURV_SCORE_PARK_CLEANLINESS AS STRING),
        'staff_friendliness', CAST(survey.SURV_SCORE_STAFF_FRIENDLINESS AS STRING),
        'wayfinding_indoor', CAST(survey.SURV_SCORE_WAYFINDING_INDOOR AS STRING),
        'value_for_money', CAST(survey.SURV_SCORE_VALUE_FOR_MONEY AS STRING),
        'free_wifi', CAST(survey.surv_score_free_wifi AS STRING),
        'yi_mobile_app', CAST(survey.surv_score_yi_mobile_app AS STRING),
        'car_parking', CAST(survey.surv_score_car_parking AS STRING),
        'call_center', CAST(survey.surv_score_call_center AS STRING),
        'yi_website', CAST(survey.surv_score_yi_website AS STRING),
        'locker_luggage', CAST(survey.surv_score_locker_luggage AS STRING)
    ) as generic_visitor_ratings,

    map(
        'aaz_szdlc', CAST(survey.surv_score_AAZ_szdlc as STRING),
        'aaz_animal_wellbeing', CAST(survey.surv_score_AAZ_animal_wellbeing as STRING),
        'aaz_premium_guided_service', CAST(survey.surv_score_AAZ_premium_guided_service as STRING),
        'aaz_safari', CAST(survey.surv_score_AAZ_safari as STRING),
        'aaz_add_ons', CAST(survey.surv_score_AAZ_add_ons as STRING),
        'aaz_restroom_changing_facility', CAST(survey.surv_score_AAZ_restroom_changing_facility as STRING),
        'aaz_ticket_counter', CAST(survey.surv_score_AAZ_ticket_counter as STRING),
        'aaz_accessbility_support', CAST(survey.surv_score_AAZ_accessbility_support as STRING),
        'aaz_stroller_service', CAST(survey.surv_score_AAZ_stroller_service as STRING),
        'aaz_maps_signage', CAST(survey.surv_score_AAZ_maps_signage as STRING),
        'aaz_shuttle_service', CAST(survey.surv_score_AAZ_shuttle_service as STRING),
        'aaz_prayer_rooms', CAST(survey.surv_score_AAZ_prayer_rooms as STRING),
        'aaz_wheelchair_service', CAST(survey.surv_score_AAZ_wheelchair_service as STRING),
        'aaz_guest_relations_desk', CAST(survey.surv_score_AAZ_guest_relations_desk as STRING),
        'aaz_entertainment', CAST(survey.surv_score_AAZ_entertainment as STRING),
        'swad_zoological_experiences', CAST(survey.surv_score_zoological_experiences as STRING),
        'swad_animal_presentations', CAST(survey.surv_score_animal_presentations as STRING),
        'swad_sea_guardian', CAST(survey.surv_score_sea_guardian as STRING),
        'swad_seaworld_expeditions', CAST(survey.surv_score_seaworld_expeditions as STRING),
        'clb_climbing_experience', CAST(survey.surv_score_clb_climbing_experience as STRING),
        'clb_flying_experience', CAST(survey.surv_score_clb_flying_experience as STRING),
        'tlp_sketch_factory', CAST(survey.surv_score_tlp_sketch_factory as STRING),
        'tlp_staff_kids_interaction', CAST(survey.surv_score_tlp_staff_kids_interaction as STRING),
        'tlp_lockers_changing_room', CAST(survey.surv_score_tlp_lockers_changing_room as STRING),
        'tlp_facilities', CAST(survey.surv_score_tlp_facilities as STRING),
        'tlp_anko_japanese', CAST(survey.surv_score_tlp_anko_japanese as STRING),
        'tlp_duration_of_visit', CAST(survey.surv_score_tlp_duration_of_visit as STRING),
        'qaw_light_show', CAST(survey.surv_score_qaw_light_show as STRING),
        'qaw_exhibition', CAST(survey.surv_score_qaw_exhibition as STRING),
        'qaw_physical_tour_guide', CAST(survey.surv_score_qaw_physical_tour_guide as STRING),
        'qaw_multimedia_tour_guide', CAST(survey.surv_score_qaw_multimedia_tour_guide as STRING),
        'qaw_shuttle_bus_service', CAST(survey.surv_score_qaw_shuttle_bus_service as STRING)
    ) as park_visitor_ratings,

    CAST(NULL AS STRING) AS positive_comments,
    CAST(NULL AS STRING) AS negative_comments,

    'vw_survey_agg_analysis_all_parks' as source_name

FROM {vw_survey_agg_analysis} survey
qualify row_number() over (partition by survey.response_id order by survey.SURV_SCORE_NPS desc, survey.SURV_SCORE_GSS desc, survey.visit_date desc) = 1
),

-- EA DATA
ea_data AS (
SELECT
    survey_ea.response_id,
    survey_ea.visit_date,
    survey_ea.response_date,
    survey_ea.facility_name,
    {email_standardization("survey_ea.email_address")} as email_address,
    survey_ea.survey_name,

    map(
        'email_id', nullif(survey_ea.email_address,'Not Available'),
        'customer_gender', survey_ea.surv_customer_gender,
        'customer_emirate', survey_ea.surv_customer_emirate,
        'customer_cor', survey_ea.surv_customer_cor,
        'customer_age', survey_ea.surv_customer_age,
        'customer_nationality', survey_ea.surv_customer_nationality,
        'customer_marital_status', survey_ea.surv_customer_marital_status
    ) as respondent_details,

    map(
        'ticket_id', survey_ea.surv_ticket_id,
        'event_name', survey_ea.surv_event_name,
        'event_venue', survey_ea.surv_event_venue,
        'ticket_purchase_platform', survey_ea.surv_ticket_purchase_platform,
        'customer_hotel_stay', survey_ea.surv_customer_hotel_stay,
        'customer_reach_channel', survey_ea.surv_customer_reach_channel,
        'customer_prev_yi_visits', survey_ea.surv_customer_prev_yi_visits
    ) as entry_details,

    CAST(survey_ea.surv_score_evnt_nps AS DOUBLE) as nps_score,
    CAST(survey_ea.surv_score_gss AS DOUBLE) as gss_score,

    CAST(NULL AS MAP<STRING, STRING>) AS generic_visitor_ratings,

    map(
        'spend_food_drink', cast(survey_ea.surv_spend_food_drink as string),
        'spend_merch', cast(survey_ea.surv_spend_merch as string),
        'spend_other', cast(survey_ea.surv_spend_other as string),
        'score_venue_cleanliness', cast(survey_ea.surv_score_venue_cleanliness as string),
        'score_seat_comfort', cast(survey_ea.surv_score_seat_comfort as string),
        'score_wayfinding_within_venue', cast(survey_ea.surv_score_wayfinding_within_venue as string),
        'score_look_feel', cast(survey_ea.surv_score_look_feel as string),
        'score_queue', cast(survey_ea.surv_score_queue as string),
        'score_ticket_booking', cast(survey_ea.surv_score_ticket_booking as string),
        'score_staff_helpfulness', cast(survey_ea.surv_score_staff_helpfulness as string),
        'score_sound_quality', cast(survey_ea.surv_score_sound_quality as string),
        'score_event_vfm', cast(survey_ea.surv_score_event_vfm as string),
        'score_fnb_vfm', cast(survey_ea.surv_score_fnb_vfm as string),
        'surv_score_yas_island_nps', cast(survey_ea.surv_score_yas_island_nps as string)
    ) as park_visitor_ratings,

    survey_ea.surv_feedback_liked as positive_comments,
    survey_ea.surv_feedback_disliked as negative_comments,

    'vw_survey_agg_analysis_ea' as source_name

FROM {schema}.vw_survey_agg_analysis_ea survey_ea
qualify row_number() over (partition by survey_ea.response_id order by CAST(survey_ea.surv_score_evnt_nps AS DOUBLE) desc, CAST(survey_ea.surv_score_gss AS DOUBLE) desc, survey_ea.visit_date desc) = 1
),

-- UNION FIRST
union_data AS (
SELECT * FROM all_park_data
UNION ALL
SELECT * FROM ea_data
)

-- FINAL JOIN
SELECT
    map('scv_sk',cast(scv.scv_account_sk as string),
      'omni_sk',NULL,
      'email_id', u.email_address
      ) as entity_identifiers,

    scv.scv_account_sk AS owner_scv_sk,
    CAST(NULL AS STRING) AS owner_omni_sk,

    u.response_id,
    u.visit_date,
    u.response_date,
    u.facility_name,
    u.survey_name,

    u.respondent_details,
    u.entry_details,

    u.nps_score,
    u.gss_score,

    u.generic_visitor_ratings,
    u.park_visitor_ratings,

    u.positive_comments,
    u.negative_comments,

    u.source_name

FROM union_data u
LEFT JOIN scv_active scv
    ON u.email_address = scv.email_id
""")

# Add ETL timestamps via DataFrame API (avoids SQL injection risk from f-string interpolation)
df_final_customer_survey = (df_final_customer_survey
    .withColumn("etl_created_datetime", F.lit(current_uae_timestamp))
    .withColumn("etl_updated_datetime", F.lit(current_uae_timestamp))
)

df_final_customer_survey = df_final_customer_survey.toDF(
    *[c.lower() for c in df_final_customer_survey.columns]
)

# COMMAND ----------

# DBTITLE 1,Schema
# Apply schema casting
df_final_customer_survey = apply_schema_casting(df_final_customer_survey, vw_survey_analysis_schema)

# COMMAND ----------

# DBTITLE 1,Load
# Build DDL string from schema dict
customer_survey_ddl = ",\n".join(
    f"{col_name} {dtype}" for col_name, dtype in vw_survey_analysis_schema.items()
)

try:
    ddl_struct = StructType.fromDDL(customer_survey_ddl)

    # Check if table exists
    table_exists = spark.catalog.tableExists(target_table)

    # watermark logic
    if table_exists:
        watermark = spark.sql(f"SELECT MAX(response_date) FROM {target_table}").collect()[0][0]
    else:
        watermark = None

    use_date_range = bool(start_date and end_date)
    is_full_load   = (not table_exists) or (watermark is None and not use_date_range)

    if not table_exists:
        print(f"Load mode  : FULL LOAD (table does not exist)")
    elif use_date_range:
        print(f"Load mode  : DATE RANGE ({start_date} to {end_date})")
    elif is_full_load:
        print(f"Load mode  : FULL LOAD")
    else:
        print(f"Load mode  : INCREMENTAL (watermark: {watermark}, buffer: {buffer_days} days)")

    # -------------------- schema mismatch helper --------------------
    def schema_mismatch_detail(df_schema, expected_struct):
        """Return a diagnostic string showing schema differences."""
        actual_fields = set(df_schema.fieldNames())
        expected_fields = set(expected_struct.fieldNames())
        missing = expected_fields - actual_fields
        extra = actual_fields - expected_fields
        details = []
        if missing:
            details.append(f"Missing columns: {sorted(missing)}")
        if extra:
            details.append(f"Extra columns: {sorted(extra)}")
        # Check type mismatches for common columns
        common = actual_fields & expected_fields
        actual_map = {f.name: f.dataType.simpleString() for f in df_schema.fields}
        expected_map = {f.name: f.dataType.simpleString() for f in expected_struct.fields}
        type_mismatches = [
            f"  {col}: expected '{expected_map[col]}', got '{actual_map[col]}'"
            for col in sorted(common) if actual_map[col] != expected_map[col]
        ]
        if type_mismatches:
            details.append("Type mismatches:\n" + "\n".join(type_mismatches))
        return "; ".join(details) if details else "Unknown mismatch"

    # -------------------- metrics initialization --------------------
    inserted_count = 0
    updated_count  = 0

    # -------------------- FULL LOAD --------------------
    if is_full_load:
        if compare_schema_lenient(df_final_customer_survey.schema, ddl_struct):
            print("Schema matches!")

            # Atomic overwrite (replaces TRUNCATE + append to avoid data loss window)
            (df_final_customer_survey.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target_table))

            # Get inserted count from target table (avoids redundant full scan of source DataFrame)
            inserted_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
            updated_count  = 0

            print("Full load complete.")
        else:
            raise Exception(f"Schema mismatch! {schema_mismatch_detail(df_final_customer_survey.schema, ddl_struct)}")

    # -------------------- DATE RANGE --------------------
    elif use_date_range:
        df_incremental = df_final_customer_survey.filter(
            (col("response_date") >= start_date) &
            (col("response_date") <= end_date)
        )

        if compare_schema_lenient(df_incremental.schema, ddl_struct):
            print("Schema matches!")

            delta_tgt = DeltaTable.forName(spark, target_table)

            df_result = (delta_tgt.alias("tgt")
                .merge(
                    df_incremental.alias("src"),
                    "tgt.response_id = src.response_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            # Metrics from merge result (avoids extra pre-merge scan)
            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

            print(f"Date range upsert complete. Inserted: {inserted_count}, Updated: {updated_count}")
        else:
            raise Exception(f"Schema mismatch! {schema_mismatch_detail(df_incremental.schema, ddl_struct)}")

    # -------------------- INCREMENTAL --------------------
    else:

        df_incremental = df_final_customer_survey.filter(
            col("response_date") >= expr(f"timestamp('{watermark}') - interval {buffer_days} days")
        )

        if compare_schema_lenient(df_incremental.schema, ddl_struct):
            print("Schema matches!")

            delta_tgt = DeltaTable.forName(spark, target_table)

            df_result = (delta_tgt.alias("tgt")
                .merge(
                    df_incremental.alias("src"),
                    "tgt.response_id = src.response_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            # Metrics from merge result (avoids extra pre-merge scan)
            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

            print(f"Incremental upsert complete. Inserted: {inserted_count}, Updated: {updated_count}")
        else:
            raise Exception(f"Schema mismatch! {schema_mismatch_detail(df_incremental.schema, ddl_struct)}")

    # Final count (safe)
    if spark.catalog.tableExists(target_table):
        raw_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]

        print(f"Customer Survey Table Loaded...")
        print(f"Rows Inserted : {inserted_count}")
        print(f"Rows Updated  : {updated_count}")
        print(f"Total Records : {raw_count}")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

#view creation
catalog     = f"transformation_{env}"
schema      = "plt_customer360"
object_name = "vw_customer_survey"
select_query = c360_view_queries[object_name].format(catalog=catalog, schema=schema)
create_view_or_table(catalog=catalog, schema=schema, object_name=object_name, select_query=select_query)

# COMMAND ----------

# add last_updated tag with current timestamp to identify table last refreshed datetime.
add_last_updated_tag(target_table)

# COMMAND ----------

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

trigger_databricks_job_by_name('workflow_orchestration')
