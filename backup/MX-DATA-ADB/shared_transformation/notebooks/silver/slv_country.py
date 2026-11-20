# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC 1. Source table: **vw_crm_dot_country**
# MAGIC
# MAGIC 2. Target silver table: **prep_dim_country**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols": ["COUNTRY_NAME"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pipeline Initialization & Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #####Enviornment parameter will be passed from the workflow

# COMMAND ----------

# dbutils.widgets.text("env","")

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Contains expected schemas for all the tables

# COMMAND ----------

# MAGIC %run ../../config/schema_config

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

bronze_crm_dot_countries = f"transformation_{env}.brz_crm.vw_crm_dot_country"

silver_schema = f"transformation_{env}.slv_shared"
silver_table = f"{silver_schema}.prep_dim_country"


# COMMAND ----------

# MAGIC %md
# MAGIC ####Read the bronze table data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Clean the data from the bronze table and return it as a DataFrame

# COMMAND ----------

dot_countries_df = spark.read.table(bronze_crm_dot_countries).select(
    "dot_countryid",
    "statuscode",
    "dot_2digitiso",
    "createdby",
    "modifiedon",
    "modifiedby",
    "dot_name",
    "createdon",
    "statecode",
)


# COMMAND ----------

from pyspark.sql.functions import col

# Filtering out the rows based on the given conditions
dot_countries_df = dot_countries_df.filter(
    col("dot_2digitiso").isNotNull() & 
    col("dot_name").rlike("^[A-Z]")
).filter(~col("dot_2digitiso").isin("B1", "S1", "S2")).dropDuplicates(["dot_name"])


# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC #####This code optimizes the creation of DataFrames by joining multiple cleaned DataFrames and selecting relevant columns.

# COMMAND ----------

# Applying transformations
from pyspark.sql.functions import when, lit

df_country_silver = dot_countries_df.select(
    when(dot_countries_df["dot_2digitiso"] == "", None).otherwise(dot_countries_df["dot_2digitiso"]).alias("COUNTRY_CODE"),
    when(dot_countries_df["dot_name"] == "", None).otherwise(dot_countries_df["dot_name"]).alias("COUNTRY_NAME"),
    when(dot_countries_df["dot_2digitiso"] == "", None).otherwise(dot_countries_df["dot_2digitiso"]).alias("ISO_COUNTRY_CODE"),
    when(dot_countries_df["dot_name"] == "", None).otherwise(dot_countries_df["dot_name"]).alias("NATIONALITY_NAME"),
    lit(None).cast("string").alias("REGION_CODE"),
    lit(None).cast("string").alias("REGION_NAME")
).distinct().sort("COUNTRY_CODE")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Selecting required columns from the cleaned DataFrame and schema casting

# COMMAND ----------

from pyspark.sql.functions import lit
df_country_silver = apply_schema_casting(df_country_silver,dim_country_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Schema Enforcement and then writing final silver table to UC

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["COUNTRY_NAME"]

# Find duplicates
duplicate_df = (
    df_country_silver
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in df_country_silver based on key {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in df_country_silver.")


# COMMAND ----------

# Build DDL string from schema dict
dim_country_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_country_schema.items()
)


try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_country_schema_ddl)
    if compare_schema_lenient(df_country_silver.schema, ddl_struct):
        print("Schema matches!")
        df_country_silver.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        # displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        # displayHTML(f"<b>Actual:</b><br><pre>{df_country_silver.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise Exception(f"FAIL: {str(e)}")  # Fails the notebook in workflow/task
