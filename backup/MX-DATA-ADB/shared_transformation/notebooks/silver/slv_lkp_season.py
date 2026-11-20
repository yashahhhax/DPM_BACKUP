# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC 1. Source table: **brz_sharepoint_list.farahflms_seasons**
# MAGIC
# MAGIC 2. Target silver table: **season**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols": ["SEASON_START_DATETIME", "SEASON_END_DATETIME", "SEASON_NAME"]

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pipeline Initialization & Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #####Enviornment parameter will be passed from the workflow

# COMMAND ----------

dbutils.widgets.text("env","")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Contains expected schemas for all the tables

# COMMAND ----------

# MAGIC %run "../../config/schema_config"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Contains all the base functions required

# COMMAND ----------

# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #####Base function call to receive the param values

# COMMAND ----------

try:
    notebook_vars = [
        'env'
    ]
    env= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Define Source and Target Tables based on the environment variable

# COMMAND ----------

bronze_season = f"transformation_{env}.brz_sharepoint_list.vw_farahflms_seasons"

silver_schema = f"transformation_{env}.slv_shared"
silver_table = f"{silver_schema}.prep_lkp_season"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read the bronze table data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Clean the data from the bronze table and return it as a DataFrame

# COMMAND ----------

cleaned_bronze_season_df = clean_and_return_df(bronze_season)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join dataframes, then select and transform columns

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime, substring, when, datediff, lit

df_season_silver = cleaned_bronze_season_df.select(
    when(col("LeisureFacilityValue") == '', None).otherwise(col("LeisureFacilityValue")).alias("FACILITY_CODE"),
    when(col("SeasonName") == '', None).otherwise(col("SeasonName")).alias("SEASON_NAME"),
    when(col("SourceMarket") == '', None).otherwise(col("SourceMarket")).alias("SOURCE_MARKET"),
    # from_unixtime(substring(col("DateFrom"), 7, 10)).alias("SEASON_START_DATETIME"),
    # from_unixtime(substring(col("DateTo"), 7, 10)).alias("SEASON_END_DATETIME")
    col("DateFrom").alias("SEASON_START_DATETIME"),
    col("DateTo").alias("SEASON_END_DATETIME")
).distinct().select(
    col("FACILITY_CODE"),
    col("SEASON_START_DATETIME"),
    col("SEASON_END_DATETIME"),
    col("SEASON_NAME"),
    (datediff(col("SEASON_END_DATETIME"), col("SEASON_START_DATETIME")) + 1).alias("SEASON_NUMBER_DAYS"),
    col("SOURCE_MARKET"),
    lit(None).alias("SEASON_DESC")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Applying schema casting

# COMMAND ----------

from pyspark.sql.functions import col, when

df_season_silver = df_season_silver.select([
    when(col(c) == '', None).otherwise(col(c)).alias(c)
    for c in df_season_silver.columns
])


df_season_silver = apply_schema_casting(df_season_silver,dim_season_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Schema Enforcement and then writing final silver table to UC

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["SEASON_START_DATETIME", "SEASON_END_DATETIME", "SEASON_NAME"]

# Find duplicates
duplicate_df = (
    df_season_silver
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in df_season_silver based on keys {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in df_season_silver.")


# COMMAND ----------

# Build DDL string from schema dict
dim_season_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_season_schema.items()
)

# print(dim_season_schema_ddl)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_season_schema_ddl)
    if compare_schema_lenient(df_season_silver.schema, ddl_struct):
        print("Schema matches!")
        df_season_silver.write.format("delta")\
                        .option("mergeSchema", True) \
                        .mode("overwrite")\
                        .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        print(ddl_struct)
        print(df_season_silver.schema)
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_season_silver.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e
