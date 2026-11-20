# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC 1. Source tables: 
# MAGIC - *brz_sharepoint_list.vw_farahflfin_revenue_transaction_type**
# MAGIC
# MAGIC 2. Target silver table: **source_system**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols": ["TRANSACTION_TYPE_CODE"],
# MAGIC         "scd_columns": ["TRANSACTION_TYPE_NAME"]

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

# %run ../../functions/mxdrr_common_functions

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

# Define Source and Target Tables
bronze_farahflfin_drr_transaction_type = f"transformation_{env}.brz_sharepoint_list.vw_farahflfin_revenue_transaction_type"

silver_schema = f"transformation_{env}.slv_shared"
silver_table = f"{silver_schema}.prep_lkp_transaction_type"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read the bronze table data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Clean the data from the bronze table and return it as a DataFrame

# COMMAND ----------

cleaned_farahflfin_drr_transaction_type_df = clean_and_return_df(bronze_farahflfin_drr_transaction_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC #####This code optimizes the creation of DataFrames by joining multiple cleaned DataFrames and selecting relevant columns.

# COMMAND ----------

from pyspark.sql.functions import col, lit, when

df_transaction_type_silver = cleaned_farahflfin_drr_transaction_type_df.select(
    col("Title").alias("TRANSACTION_TYPE_CODE"),
    col("TRANSACTION_TYPE_NAME").alias("TRANSACTION_TYPE_NAME")
).distinct()

# COMMAND ----------

from pyspark.sql.functions import col, when

df_transaction_type_silver = df_transaction_type_silver.select([
    when(col(c) == '', None).otherwise(col(c)).alias(c)
    for c in df_transaction_type_silver.columns
])


# Apply casting to the final DataFrame using the merged schema
transaction_type_df = apply_schema_casting(df_transaction_type_silver, dim_transaction_type_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Schema Enforcement and then writing final silver table to UC

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["TRANSACTION_TYPE_CODE"]

# Find duplicates
duplicate_df = (
    transaction_type_df
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in transaction_type_df based on key {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in transaction_type_df.")


# COMMAND ----------

# Build DDL string from schema dict
dim_transaction_type_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_transaction_type_schema.items()
)

# print(dim_transaction_type_schema_ddl)


try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_transaction_type_schema_ddl)
    if compare_schema_lenient(transaction_type_df.schema, ddl_struct):
        print("Schema matches!")
        transaction_type_df.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{transaction_type_df.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e
