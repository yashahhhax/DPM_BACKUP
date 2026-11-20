# Databricks notebook source
# MAGIC %md
# MAGIC "primary_key_cols": ["MKT_SEGMENT_CODE", "MKT_SEGMENT_GROUP_CODE","LINE_OF_BUSINESS_ID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import necessary libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, NumericType
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enviornment parameter will be passed from the workflow

# COMMAND ----------

# dbutils.widgets.text("env","")

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contains the expected schemas

# COMMAND ----------

# MAGIC %run "../../config/schema_config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contains all the base functions required

# COMMAND ----------

# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

### %run ../../functions/mxdrr_common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Base function call to receive the param values

# COMMAND ----------

try:
    notebook_vars = [
        'env'
    ]
    env= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

print(env)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Source and Target Tables

# COMMAND ----------

# DBTITLE 1,defining sources
# Define the brz table name
brz_sharepoint_market_segments = f"transformation_{env}.brz_sharepoint_list.vw_farahflfin_market_segments"

# Define the silver schema and table names
slv_schema = f"transformation_{env}.slv_shared"

# Silver table name
silver_table = f"transformation_{env}.slv_shared.prep_dim_market_segments"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read and Clean Source Bronze Tables

# COMMAND ----------

# Clean source data
market_segments_df = clean_and_return_df(brz_sharepoint_market_segments)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select and Transform Required Columns

# COMMAND ----------

market_segments_clean_df = market_segments_df.select(
    F.col("MARKET_SEGMENT_CODE").alias("MKT_SEGMENT_CODE"),
    F.col("MARKET_SEGMENT_NAME").alias("MKT_SEGMENT_NAME"),
    F.col("MARKET_SEGMENT_GROUP_CODE").alias("MKT_SEGMENT_GROUP_CODE"),
    F.col("MARKET_SEGMENT_GROUP_NAME").alias("MKT_SEGMENT_GROUP_NAME")
).distinct()

# COMMAND ----------

# Then apply the transformations to the renamed columns
market_segments_clean_df = market_segments_clean_df \
    .withColumn("MKT_SEGMENT_CODE", 
                when(col("MKT_SEGMENT_CODE").isNull() | (col("MKT_SEGMENT_CODE") == ""), None)
                .otherwise(trim(upper(col("MKT_SEGMENT_CODE"))))) \
    .withColumn("MKT_SEGMENT_NAME", 
                when(col("MKT_SEGMENT_NAME").isNull() | (col("MKT_SEGMENT_NAME") == ""), None)
                .otherwise(trim(col("MKT_SEGMENT_NAME")))) \
    .withColumn("MKT_SEGMENT_GROUP_CODE", 
                when(col("MKT_SEGMENT_GROUP_CODE").isNull() | (col("MKT_SEGMENT_GROUP_CODE") == ""), None)
                .otherwise(trim(col("MKT_SEGMENT_GROUP_CODE")))) \
    .withColumn("MKT_SEGMENT_GROUP_NAME", 
                when(col("MKT_SEGMENT_GROUP_NAME").isNull() | (col("MKT_SEGMENT_GROUP_NAME") == ""), None)
                .otherwise(trim(col("MKT_SEGMENT_GROUP_NAME"))))
    

    # Add the LINE_OF_BUSINESS column 
market_segments_clean_df = market_segments_clean_df \
    .withColumn("SRC_DELETED", lit(None)).withColumn("LINE_OF_BUSINESS_ID", lit("MX")).select("MKT_SEGMENT_CODE", "MKT_SEGMENT_GROUP_CODE", "LINE_OF_BUSINESS_ID", "MKT_SEGMENT_NAME", "MKT_SEGMENT_GROUP_NAME", "SRC_DELETED")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Schema Casting

# COMMAND ----------

from pyspark.sql.functions import col, when

market_segments_clean_df = market_segments_clean_df.select([
    when(col(c) == '', None).otherwise(col(c)).alias(c)
    for c in market_segments_clean_df.columns
])


final_df = apply_schema_casting(market_segments_clean_df, dim_market_segments_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the Clean Final Data into the Silve Table

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["MKT_SEGMENT_CODE", "MKT_SEGMENT_GROUP_CODE", "LINE_OF_BUSINESS_ID"]

# Find duplicates
duplicate_df = (
    final_df
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in final_df based on keys {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in final_df.")


# COMMAND ----------

# Build DDL string from schema dict
dim_market_segments_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_market_segments_schema.items()
)

# print(dim_market_segments_schema_ddl)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_market_segments_schema_ddl)
    if compare_schema_lenient(final_df.schema, ddl_struct):
        print("Schema matches!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{final_df.schema}</pre>")
        final_df.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{final_df.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e
