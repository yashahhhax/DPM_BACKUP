# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC This notebook summarizes the process of loading and transforming hotel ticket data from the Gold table (`gld_travelbox.dwh_vw_sales`) into the Platinum table (`location_intelligence`). Schema enforcement is applied to ensure data quality. The load strategy is to be determined.
# MAGIC 1. Source Gold Table: 
# MAGIC | Schema | Table |
# MAGIC | --- | --- |
# MAGIC |gld_travelbox |dwh_vw_sales |
# MAGIC
# MAGIC 2. Target Platinum table: **location_intelligence**
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
# MAGIC %run ../../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include base fucntions
# MAGIC %run ../../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,include table config
# MAGIC %run ../../../config/table_config

# COMMAND ----------

# DBTITLE 1,include ddl
# MAGIC %run ../../../config/view_queries

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
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo


# COMMAND ----------

# DBTITLE 1,initialize bronze and silver tables
#source table
travelbox_vw_sales = f"transformation_{env}.gld_travelbox.dwh_vw_sales"

#target table
target_table = f"transformation_{env}.plt_customer360.location_intelligence"
#scv master
scv_account_master = f"transformation_{env}.plt_customer360.scv_account_master"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table: {travelbox_vw_sales}")
print(f"Target table: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,Transformation
df_location_inteligence_hotel = spark.sql(f"""
    -- Select hotel stay records for location intelligence
    WITH scv_account_master AS (
        SELECT 
            scv_account_sk,
            email_id
        FROM {scv_account_master}
        WHERE source_name = 't_mdm_guest'
        QUALIFY row_number() over(partition by email_id order by modified_date desc) = 1
    ),
    hotel_bookings_agg AS (
        SELECT
            `Booking No`,
            listagg(concat(`Product Name`, ' : ', quantity), ';') 
                FILTER(WHERE IFF(`Theme Park Flag` = 'Y','Y','N') = 'N') AS additional_booking_details,
            listagg(concat(`Product Name`, ' : ', quantity), ';') 
                FILTER(WHERE IFF(`Theme Park Flag` = 'Y','Y','N') = 'Y') AS theme_park_booking_details
        FROM (
            SELECT
                `Booking No`,
                `Product Name`,
                `Theme Park Flag`,
                count(1) AS quantity
            FROM {travelbox_vw_sales}
            WHERE `Hotel Flag` = 'N'
            GROUP BY `Booking No`, `Product Name`, `Theme Park Flag`
        ) booking_counts
        GROUP BY `Booking No`
    ),
    hotel_raw_data AS (
        SELECT
            --// Dimensions
            {email_standardization("hotel_booking.`Email address`")} AS EMAIL_ADDRESS,
            hotel_booking.`Email address`,--raw_email
            hotel_booking.`Booking No`,
            hotel_booking.`Source System`,
            COALESCE(hotel_booking.`Hotel Name B2B Hotel`, hotel_booking.`Hotel Name`) as `Hotel Name`,
            hotel_booking.`Check In Date`,
            hotel_booking.`Distribution Channel`,
            -- hotel_booking.`Partner Name`,
            hotel_booking.`Hotel Flag`,
            -- hotel_booking.`Cancellation Date`,
            hotel_booking.`Brand Name`,
            trim(hotel_booking.`Room Type Name`) as `Room Type Name`,

            --///metrics
            max(hotel_booking.`Check Out Date`) as `Check Out Date`,
            sum(hotel_booking.`Number of Rooms`) as `Number of Rooms`,
            max(hotel_booking.`Room Nights`) as `Room Nights`,
            max(hotel_booking.`Length of Stay`) as `Length of Stay`,
            sum(hotel_booking.`PAX in a booking`) as `PAX in a booking`,
            sum(hotel_booking.`Adults`) as `Adults`,
            sum(hotel_booking.`Infants`) as `Infants`,
            sum(hotel_booking.`Children`) as `Children`

        FROM {travelbox_vw_sales} hotel_booking
        WHERE 
            `Hotel Flag` = "Y"
            AND `Cancellation Date` IS NULL
            AND COALESCE(`Hotel Name B2B Hotel`, `Hotel Name`) NOT IN ("TEST Hotel")
            AND COALESCE(`Hotel Name B2B Hotel`, `Hotel Name`) IS NOT NULL
            AND (
                `Distribution Channel` NOT IN ('Do Not Use') 
                OR `Distribution Channel` IS NULL
            )
            AND (
                `Partner Name` NOT IN (
                    'YIhotelstest', 'Yasproducttestaccount', 'Alexander Test', 'cgTest', 
                    'EH TEST HOTEL', 'MD Alameri TEST', 'PROD TEST', 'Rebin Test', 
                    'Rebin Test Sub User', 'Sophia Test', 'Test Hotel',  
                    'Test Karin Seifert'
                ) 
                OR `Partner Name` IS NULL
            )
            AND COALESCE(`Hotel Name B2B Hotel`, `Hotel Name`) NOT IN (
                'Yas Generic Test', 'Test Supplier', 'TEST Hotel'
            )
            --removing future hotel booking
            AND date(`Check In Date`) <= current_date()
            GROUP BY ALL
    )
    SELECT 
        map('scv_sk',cast(scv.scv_account_sk as string),
          'omni_sk',NULL,
          'email_id', cast( hotel.`Email address` as string)
          ) as entity_identifiers,
        NULL AS purchaser_omni_sk,
        scv.scv_account_sk AS purchaser_scv_sk,
        NULL AS owner_omni_sk,
        scv.scv_account_sk AS owner_scv_sk,
        NULL AS fl_scv_sk,
        "HOTEL" AS CATEGORY,
        hotel.`Hotel Name` AS ASSET_NAME,
        hotel.`Check In Date` AS CHECKIN_TIMESTAMP,
        timestampadd(DAY, cast(hotel.`Length of Stay` as int) + 1, `Check In Date`) as CHECKOUT_TIMESTAMP,
        -- hotel.`Check Out Date` AS CHECKOUT_TIMESTAMP,
        NULL AS ANNUAL_PASS_FLAG,
        "HOTEL_STAY" AS PRODUCT_NAME,
        hotel.`Source System` AS source_name,
        MAP(
            'email_address', hotel.`Email address`, --raw_email
            'number_of_rooms', CAST(hotel.`Number of Rooms` AS STRING),
            'room_nights', CAST(hotel.`Room Nights` AS STRING),
            'length_of_stay', CAST(hotel.`Length of Stay` AS STRING),
            'pax_in_booking', CAST(hotel.`PAX in a booking` AS STRING),
            'adult_count', CAST(hotel.`Adults` AS STRING),
            'child_count', CAST(hotel.`Children` AS STRING),
            'infant_count', CAST(hotel.`Infants` AS STRING),
            'booking_no', CAST(hotel.`Booking No` AS STRING),
            'promotion_name',CAST(hotel.`Brand Name` AS STRING),
            'sale_channel',CAST(hotel.`Distribution Channel` AS STRING),
            'room_type',CAST(hotel.`Room Type Name` AS STRING),
            'additional_booking_details',hotel_bookings_agg.additional_booking_details,
            'theme_park_booking_details',hotel_bookings_agg.theme_park_booking_details
        ) AS OTHERS
    FROM hotel_raw_data hotel
    LEFT JOIN scv_account_master scv ON scv.email_id = hotel.EMAIL_ADDRESS
    LEFT JOIN hotel_bookings_agg ON hotel_bookings_agg.`Booking No` = hotel.`Booking No`
""")

df_location_inteligence_hotel = map_asset_category(df_location_inteligence_hotel, "ASSET_NAME")

df_location_inteligence_hotel.createOrReplaceTempView("hotel_raw")

# COMMAND ----------

# DBTITLE 1,final df
df_location_inteligence_hotel = spark.sql(f"""
select 
  h.entity_identifiers
  ,h.purchaser_scv_sk
  ,h.purchaser_omni_sk
  ,h.owner_scv_sk
  ,h.owner_omni_sk 
  ,h.fl_scv_sk
  ,h.category
  ,h.asset_name
  ,h.checkin_timestamp
  ,h.checkout_timestamp
  ,h.annual_pass_flag
  ,h.product_name
  ,case when h.purchaser_scv_sk is null and
            h.purchaser_omni_sk is null and
            h.owner_scv_sk is null and
            h.fl_scv_sk is null and
            h.owner_omni_sk is null then 'N'
            else 'Y' end as valid_sk  -- all sk are null then N else Y
  ,h.source_name
  ,'{current_uae_timestamp}' as etl_created_datetime
  ,'{current_uae_timestamp}' as etl_updated_datetime
  ,h.others
  from hotel_raw h
  """)

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_location_inteligence_hotel = apply_schema_casting(df_location_inteligence_hotel,location_inteligence_schema )


# COMMAND ----------

# DBTITLE 1,Write to UC
# Build DDL string from schema dict
location_inteligence_dll = ",\n".join(
    f"{col} {dtype}" for col, dtype in location_inteligence_schema.items()
)
 
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(location_inteligence_dll)
    if compare_schema_lenient(df_location_inteligence_hotel.schema, ddl_struct):
        print("Schema matches!")

        deleted_count = 0
        inserted_count = 0

        if spark.catalog.tableExists(target_table):
            # Count rows to be deleted
            deleted_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table} WHERE CATEGORY = 'HOTEL'").collect()[0]['cnt']
            #delete existing data where category is 'HOTEL'
            spark.sql(f"DELETE FROM {target_table} WHERE CATEGORY = 'HOTEL'")
            print("Deleting existing data")
            
        # Write data to table
        (df_location_inteligence_hotel.write.format("delta")
                        .mode("append")
                        .saveAsTable(target_table))
        
        # Get inserted row count from Delta table history — avoids double computation
        history_metrics = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").select("operationMetrics").collect()[0][0]
        inserted_count = int(history_metrics.get("numOutputRows", 0))
        
        print("Location Intlligence Hotel Data Loaded")
        print(f"Rows Deleted: {deleted_count}")
        print(f"Rows Inserted: {inserted_count}")
        
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_location_inteligence_hotel.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,tbl readiness - update tag
readiness_table = f"transformation_{env}.plt_customer360.readiness_location_intelligence_hotel"

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
 
