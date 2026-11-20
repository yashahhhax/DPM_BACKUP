# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC
# MAGIC **The `group_kpi` table is the group-level KPI data product consolidating aggregated churn, spend, and stay KPIs derived from customer-level KPI metrics.**
# MAGIC
# MAGIC 1. Source Tables:
# MAGIC
# MAGIC | Schema | Table | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | plt_customer360 | customer_kpi | Source customer-level KPI metrics used for churn, spend, and stay aggregations |
# MAGIC | plt_customer360 | master_kpi | KPI ID lookup and KPI metadata mapping |
# MAGIC
# MAGIC 2. Target Platinum table: **transformation_{env}.plt_customer360.group_kpi**
# MAGIC
# MAGIC 3. Load Strategy: **Full Load (Run 1: DELETE + INSERT) \| Incremental (Run 2: Delta MERGE — upsert on report_date + kpi_id)**
# MAGIC
# MAGIC 4. Table Design: **Narrow KPI table — one row per KPI per reporting date**
# MAGIC
# MAGIC 5. Composite Merge Key: **report_date + kpi_id**
# MAGIC
# MAGIC 6. REPORT_HOUR: **Always 0 for daily KPI snapshots — column reserved for future hourly KPI processing**
# MAGIC
# MAGIC 7. KPI Processing Logic: **Pivoted KPI metrics are aggregated, unpivoted, and mapped to KPI IDs using master_kpi**
# MAGIC
# MAGIC 8. Incremental Strategy: **MERGE updates existing KPI values and inserts new KPI records using Delta Lake merge semantics**
# MAGIC
# MAGIC 9. Watermark Logic: **MAX(report_date) from target table used to determine full vs incremental load**
# MAGIC
# MAGIC 10. Duplicate Validation: **Duplicate merge-key validation implemented before incremental MERGE execution**
# MAGIC
# MAGIC 11. Optimization Strategy: **OPTIMIZE + ZORDER BY (report_date, group_name, kpi_id)**
# MAGIC
# MAGIC 12. Schema Enforcement implemented

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include base functions
# MAGIC %run ../../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,import dependencies
from pyspark.sql import functions as F
from datetime import datetime, timedelta, date as _date
from zoneinfo import ZoneInfo
from delta.tables import DeltaTable 
from pyspark.sql.functions import col, lit, to_date, to_timestamp, current_timestamp
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
import traceback

# COMMAND ----------

# dbutils.widgets.text("env","dev")
# dbutils.widgets.text("current_job_run_id","")
# dbutils.widgets.text("report_date","")              
# env                = dbutils.widgets.get("env")
# current_job_run_id = dbutils.widgets.get("current_job_run_id")
# report_date        = dbutils.widgets.get("report_date")
# print(f"env                : {env}")
# print(f"current_job_run_id : {current_job_run_id}")
# print(f"report_date        : {report_date}")

# COMMAND ----------

# DBTITLE 1,get notebook parameters
try:
    notebook_vars = ['env', 'current_job_run_id', 'report_date']
    env, current_job_run_id, report_date = get_notebook_params(
        *notebook_vars,
        verbose=True
    )
except Exception as e:
    print(f"Error: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Global error handler
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
KPI_DATE = report_date.strip() if report_date and report_date.strip() else str(_date.today() - timedelta(days=1))
print(f"report_date resolved : {KPI_DATE}")

tgt_db = f"transformation_{env}.plt_customer360"

# source tables
src_customer_kpi = f"{tgt_db}.customer_kpi"
src_master_kpi = f"{tgt_db}.master_kpi"

# target table
target_table = f"{tgt_db}.group_kpi"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table : {src_customer_kpi}")
print(f"Source table : {src_master_kpi}")
print(f"Target table : {target_table}")
print(f"UAE timestamp : {current_uae_timestamp}")


# COMMAND ----------

# DBTITLE 1,Data Check
row_count = spark.sql(f"""
    SELECT COUNT(1) AS cnt
    FROM {src_customer_kpi}
    WHERE report_date = DATE(:kpi_date)
      AND identifier_type = 'email_id'
""", args={"kpi_date": KPI_DATE}).collect()[0]['cnt']

if row_count == 0:
    raise Exception(f"No data found in {src_customer_kpi} for report_date = {KPI_DATE}. Exiting notebook.")

# COMMAND ----------

# DBTITLE 1,load kpi master reference
spark.sql(f"""
    select kpi_id, upper(kpi_name) as kpi_name
    from {src_master_kpi}
    where kpi_level   = 'group'
      and active_flag = true
""").createOrReplaceTempView("vw_master_kpi")
print("vw_master_kpi tempview created.")

# COMMAND ----------

# DBTITLE 1,Pivoting with Master KPI
df_final = (
    spark.sql(f"""
        SELECT
            report_date,
            report_hour,
            c.KPI_ID,
            KPI_VALUE,
            KPI_NAME,
            KPI_DESCRIPTION,
            identifier_value
        FROM {src_customer_kpi} c
        JOIN {src_master_kpi} m
            ON c.kpi_id = m.kpi_id
        WHERE c.identifier_type = 'email_id'
        AND report_date = DATE(:kpi_date)
    """, args={"kpi_date": KPI_DATE})

    # -------------------- pivot --------------------
    # Using F.max for deterministic results — if duplicates exist per
    # (report_date, report_hour, KPI_ID, identifier_value, KPI_NAME),
    # the highest KPI_VALUE is consistently selected across runs.
    .groupBy(
        "report_date",
        "report_hour",
        "KPI_ID",
        "identifier_value"
    )
    .pivot("KPI_NAME")
    .agg(F.max("KPI_VALUE"))
)

# -------------------- base columns --------------------
base_cols = [
    "report_date",
    "report_hour",
    "KPI_ID",
    "identifier_value"
]

# -------------------- dynamic column selection -------------------- 
selected_cols = base_cols + [
    c for c in df_final.columns
    if c == "CHURN_STATUS"
    or c.startswith("AVG_SPENT_VISIT_LAST_")
    or c.startswith("AVG_LENGTH_OF_STAY_YAS")
]

df_final = df_final.select(*selected_cols)

df_final.createOrReplaceTempView("df_group_kpi_data")

# COMMAND ----------

# DBTITLE 1,Define Group Kpi Data
df_churn_status = spark.sql(f"""
SELECT
    report_date,
    COUNT(CASE WHEN CHURN_STATUS = 'returnee' THEN 1 END) AS number_returnee,
    COUNT(CASE WHEN CHURN_STATUS = 'churned' THEN 1 END) AS number_churns,
    COUNT(CASE WHEN CHURN_STATUS = 'newly_acquired_customer' THEN 1 END) AS number_new_acquires,
    COUNT(CASE WHEN CHURN_STATUS = 'active' THEN 1 END) AS number_active,
    COUNT(CASE WHEN CHURN_STATUS = 'inactive' THEN 1 END) AS number_inactive,
    COUNT(CASE WHEN CHURN_STATUS IN ('active','returnee','newly_acquired_customer') THEN 1 END) AS closing_base

FROM df_group_kpi_data

GROUP BY report_date
""").dropDuplicates()
df_churn_status.createOrReplaceTempView("churn_df")

# COMMAND ----------

def calculate_avg_multiple_cols(df, group_col, col_list):
    agg_exprs = [
        F.avg(F.col(col_name)).alias(f"{col_name.lower()}")
        for col_name in col_list
    ]

    return df.groupBy(group_col).agg(*agg_exprs)

# COMMAND ----------

df_avg = calculate_avg_multiple_cols(
    df_final,
    ["report_date"],
    [
        "AVG_SPENT_VISIT_LAST_12_MONTHS",
        "AVG_SPENT_VISIT_LAST_1_MONTHS",
        "AVG_SPENT_VISIT_LAST_3_MONTHS",
        "AVG_SPENT_VISIT_LAST_6_MONTHS",
        "AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_12MTHS",
        "AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_1MTHS",
        "AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_3MTHS",
        "AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_6MTHS"
    ]
)
df_avg.createOrReplaceTempView("avg_df")

# COMMAND ----------

# DBTITLE 1,pivoting group kpi
df_c360_group_kpis = spark.sql(f"""
SELECT
    -- churn metrics
    c.report_date,
    c.number_returnee,
    c.number_churns,
    c.number_new_acquires,
    c.number_active,
    c.number_inactive,
    c.closing_base,

    -- avg metrics
    a.avg_spent_visit_last_12_months,
    a.avg_spent_visit_last_1_months,
    a.avg_spent_visit_last_3_months,
    a.avg_spent_visit_last_6_months,
    a.avg_length_of_stay_yas_in_seconds_12mths,
    a.avg_length_of_stay_yas_in_seconds_1mths,
    a.avg_length_of_stay_yas_in_seconds_3mths,
    a.avg_length_of_stay_yas_in_seconds_6mths

FROM churn_df c
LEFT JOIN (
    SELECT
        report_date,
        avg_spent_visit_last_12_months,
        avg_spent_visit_last_1_months,
        avg_spent_visit_last_3_months,
        avg_spent_visit_last_6_months,
        avg_length_of_stay_yas_in_seconds_12mths,
        avg_length_of_stay_yas_in_seconds_1mths,
        avg_length_of_stay_yas_in_seconds_3mths,
        avg_length_of_stay_yas_in_seconds_6mths
    FROM avg_df
) a
ON c.report_date = a.report_date
""").createOrReplaceTempView("vw_group_kpi_pivoted")

# COMMAND ----------

kpi_columns = [c for c in spark.table("vw_group_kpi_pivoted").columns if c != "report_date"]
map_expr    = ", ".join([f"'{c.upper()}', cast(`{c}` as string)" for c in kpi_columns])

spark.sql(f"""
    select
         report_date
        ,0       as report_hour
        ,'Miral' as group_name
        ,kpi_name
        ,kpi_value
    from vw_group_kpi_pivoted
    lateral view explode(map({map_expr})) t as kpi_name, kpi_value
""").createOrReplaceTempView("vw_group_kpi_unpivoted")

# COMMAND ----------

# DBTITLE 1,unpivoting group kpi
df_c360_group_kpis = spark.sql("""
    select
        n.report_date
        ,n.report_hour
        ,n.group_name
        ,m.kpi_id
        ,n.kpi_value
    from vw_group_kpi_unpivoted n
    inner join vw_master_kpi m on n.kpi_name = m.kpi_name
    where n.kpi_value is not null
""")

# COMMAND ----------

# DBTITLE 1,schema casting
df_c360_group_kpis = apply_schema_casting(df_c360_group_kpis, group_kpis_schema)

# COMMAND ----------

# DBTITLE 1,write to UC
group_kpis_ddl = ",\n".join(
    f"{col_name} {dtype}" for col_name, dtype in group_kpis_schema.items()
)

try:
    ddl_struct = StructType.fromDDL(group_kpis_ddl)

    df_c360_group_kpis = (
        df_c360_group_kpis
        .withColumn("created_at", to_timestamp(lit(current_uae_timestamp)))
        .withColumn("modified_at", to_timestamp(lit(current_uae_timestamp)))
    )

    if compare_schema_lenient(df_c360_group_kpis.schema, ddl_struct):

        print("Schema matches!")

        inserted_count = 0
        updated_count  = 0
        
        table_exists = spark.catalog.tableExists(target_table)

        # -------------------- watermark --------------------
        if table_exists:
            watermark = spark.sql(
                f"SELECT MAX(report_date) FROM {target_table}"
            ).collect()[0][0]
        else:
            watermark = None

        is_full_load = watermark is None

        # -------------------- FULL LOAD --------------------
        if is_full_load:

            print("Load mode  : FULL LOAD")

            # Atomic overwrite — safe against partial failures
            (
                df_c360_group_kpis
                .write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target_table)
            )

            inserted_count = spark.sql(
                f"SELECT COUNT(*) FROM {target_table}"
            ).collect()[0][0]

            updated_count = 0

            # -------------------- retention --------------------
            # spark.sql(f"""
            #     DELETE FROM {target_table}
            #     WHERE report_date < date_sub('{report_date}', 7)
            # """)

            # -------------------- optimize --------------------
            spark.sql(f"""
                OPTIMIZE {target_table}
                ZORDER BY (report_date, group_name, kpi_id)
            """)

        # -------------------- INCREMENTAL LOAD --------------------
        else:

            print(f"Load mode  : INCREMENTAL (report_date: {report_date})")

        # -------------------- duplicate check --------------------
            dup_df = (
                df_c360_group_kpis
                .groupBy(
                    "report_date",
                    "report_hour",
                    "group_name",
                    "kpi_id"
                )
                .count()
                .filter("count > 1")
            )

            # Short-circuit: stops scanning as soon as one duplicate is found
            assert dup_df.isEmpty(), \
                "Duplicate merge keys found in source dataframe"

            delta_tgt = DeltaTable.forName(spark, target_table)

            df_result = (
                delta_tgt.alias("tgt")
                .merge(
                    df_c360_group_kpis.alias("src"),
                    """
                    tgt.report_date = src.report_date
                    AND tgt.kpi_id = src.kpi_id
                    """
                )
                .whenMatchedUpdate(set={
                    "kpi_value"   : "src.kpi_value",
                    "modified_at" : "src.modified_at"
                })
                .whenNotMatchedInsertAll()
                .execute()
            )

            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

            # # -------------------- retention --------------------
            # spark.sql(f"""
            #     DELETE FROM {target_table}
            #     WHERE report_date < date_sub('{report_date}', 7)
            # """)

            # -------------------- optimize --------------------
            spark.sql(f"""
                OPTIMIZE {target_table}
                ZORDER BY (report_date, group_name, kpi_id)
            """)

        print("Group KPI Table Loaded...")
        print(f"Rows Inserted : {inserted_count}")
        print(f"Rows Updated  : {updated_count}")

    else:

        print("Schema mismatch!")

        displayHTML(
            f"<b>Expected:</b><br><pre>{ddl_struct}</pre>"
        )

        displayHTML(
            f"<b>Actual:</b><br><pre>{df_c360_group_kpis.schema}</pre>"
        )

        raise Exception(
            "Schema mismatch between actual and expected."
        )


except Exception as e:
    msg = f"FAILED: [write to UC] {str(e)}"
    print(msg)
    dbutils.notebook.exit(msg)

# COMMAND ----------

# DBTITLE 1,update_latest_tag
add_last_updated_tag(target_table)

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

print("Group KPI notebook completed successfully.")
dbutils.notebook.exit("SUCCESS")
