# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC **The `master_kpi` table is a reference table that maintains the master list of all KPI definitions across all levels (asset, customer, group).**
# MAGIC
# MAGIC 1. Target Platinum table: **transformation_dev.plt_customer360.master_kpi**
# MAGIC
# MAGIC 2. Load Strategy: **Full Load (Run 1: TRUNCATE + INSERT) | Incremental (Run 2: MERGE on kpi_name — preserves kpi_id)**
# MAGIC
# MAGIC 3. KPI Level values: **asset | customer | group**
# MAGIC
# MAGIC 4. Source: **transformation_{env}.brz_sharepoint_file.vw_customer360_master_kpi_list** (Sharepoint via ADF)
# MAGIC
# MAGIC 5. New columns from bronze: **kpi_id** (pre-assigned from Sharepoint), **source_table**
# MAGIC
# MAGIC 5. Schema Enforcement implemented

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
dbutils.widgets.text("env","dev")
dbutils.widgets.text("current_job_run_id","")
env                = dbutils.widgets.get("env")
current_job_run_id = dbutils.widgets.get("current_job_run_id")
print(f"env                : {env}")
print(f"current_job_run_id : {current_job_run_id}")

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
        'env', 'current_job_run_id'
    ]
    env, current_job_run_id = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,import  dependencies
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, to_timestamp
from delta.tables import DeltaTable
from datetime import datetime
from zoneinfo import ZoneInfo

# COMMAND ----------

# DBTITLE 1,initialize target table
# source bronze table — KPI master from Sharepoint via ADF
src_master_kpi = f"transformation_{env}.brz_sharepoint_file.vw_customer360_master_kpi_list"

target_table = f"transformation_{env}.plt_customer360.master_kpi"
print(f"Source table : {src_master_kpi}")
print(f"Target table : {target_table}")

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")
print(f"UAE timestamp : {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,master_kpi table
# MAGIC %skip
# MAGIC spark.sql(f"""
# MAGIC     CREATE TABLE IF NOT EXISTS {target_table} (
# MAGIC         kpi_id          BIGINT,             -- from bronze Sharepoint source
# MAGIC         kpi_name        STRING      NOT NULL,
# MAGIC         kpi_description STRING,
# MAGIC         active_flag     BOOLEAN     NOT NULL,
# MAGIC         kpi_level       STRING      NOT NULL,
# MAGIC         source_table    STRING,             -- platinum table that feeds this KPI
# MAGIC         created_at      TIMESTAMP,
# MAGIC         modified_at     TIMESTAMP
# MAGIC     )
# MAGIC     USING DELTA
# MAGIC """)
# MAGIC print(f"Table created (or already exists) : {target_table}")

# COMMAND ----------

# DBTITLE 1,master kpi data
# DBTITLE 1,master kpi data
#--------------------Read KPI definitions from bronze Sharepoint source--------------------
df_master_kpi = spark.sql(f"""
    select
        kpi_id                                      
        ,kpi_name
        ,kpi_description
        ,active_flag                               
        ,kpi_level
        ,source_table                              
    from {src_master_kpi}
    where kpi_name  is not null
      and kpi_level is not null
""")

#--------------------dedup on kpi_name + kpi_level — keep latest kpi_id from Sharepoint--------------------
df_master_kpi = df_master_kpi.dropDuplicates(["kpi_name", "kpi_level"])

# COMMAND ----------

# DBTITLE 1,schema casting
# kpi_id and source_table both in kpi_master_schema — apply casting directly
df_master_kpi = apply_schema_casting(df_master_kpi, kpi_master_schema)

# COMMAND ----------

# DBTITLE 1,write to UC
#-------kpi_id from bronze Sharepoint source — Sharepoint is source of truth--------------------
kpi_master_ddl = ",\n".join(
    f"{col_name} {dtype}" for col_name, dtype in kpi_master_schema.items()
)

try:
    ddl_struct    = StructType.fromDDL(kpi_master_ddl)
    df_master_kpi = df_master_kpi \
        .withColumn("created_at",  to_timestamp(lit(current_uae_timestamp))) \
        .withColumn("modified_at", to_timestamp(lit(current_uae_timestamp)))

    if compare_schema_lenient(df_master_kpi.schema, ddl_struct):
        print("Schema matches!")

        #--------------------initialize counts--------------------
        inserted_count = 0
        updated_count  = 0

        #--------------------check if table exists--------------------
        table_exists = spark.catalog.tableExists(target_table)

        #--------------------get watermark--------------------
        if table_exists:
            watermark = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
        else:
            watermark = 0
        is_full_load = watermark == 0

        if is_full_load:
            print("Load mode : FULL LOAD")
            if table_exists:
                spark.sql(f"TRUNCATE TABLE {target_table}")
            (df_master_kpi.write.format("delta").mode("append")
                .saveAsTable(target_table))
            inserted_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
            updated_count  = 0
        else:
            print("Load mode : MERGE")
            delta_tgt = DeltaTable.forName(spark, target_table)
            df_result = (delta_tgt.alias("tgt")
                .merge(df_master_kpi.alias("src"),
                       "upper(tgt.kpi_name) = upper(src.kpi_name) AND upper(tgt.kpi_level) = upper(src.kpi_level)")
                .whenMatchedUpdate(set={
                    "kpi_id"          : "src.kpi_id",
                    "kpi_description" : "src.kpi_description",
                    "active_flag"     : "src.active_flag",
                    "kpi_level"       : "src.kpi_level",
                    "source_table"    : "src.source_table",
                    "modified_at"     : "src.modified_at"
                })
                .whenNotMatchedInsert(values={
                    "kpi_id"          : "src.kpi_id",
                    "kpi_name"        : "src.kpi_name",
                    "kpi_description" : "src.kpi_description",
                    "active_flag"     : "src.active_flag",
                    "kpi_level"       : "src.kpi_level",
                    "source_table"    : "src.source_table",
                    "created_at"      : "src.created_at",
                    "modified_at"     : "src.modified_at"
                })
                .execute())
            result_row     = df_result.collect()[0]
            inserted_count = result_row["num_inserted_rows"]
            updated_count  = result_row["num_updated_rows"]

        print(f"Master KPI Table Loaded...")
        print(f"Rows Inserted : {inserted_count}")
        print(f"Rows Updated  : {updated_count}")
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_master_kpi.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,update_latest_tag
add_last_updated_tag(target_table)

# COMMAND ----------

# DBTITLE 1,plt_logging
plt_stats = {
    "gld_table"                   : target_table,
    "dim_new_records_count"       : inserted_count,
    "dim_updated_records_count"   : updated_count,
    "fact_inserted_records_count" : 0,
    "fact_updated_records_count"  : 0
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)
