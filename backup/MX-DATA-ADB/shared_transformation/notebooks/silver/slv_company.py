# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC 1. Source table: **vw_farahflfin_companydimension** and **vw_farahflfin_coa_segment_mapping**
# MAGIC
# MAGIC 2. Target silver table: **prep_dim_company**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols": ["OMNI_SITEID", "IG_SITEID", "COMPANY_ID", "COMPANY_ID_NEW"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pipeline Initialization & Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC #####Environment parameter will be passed from the workflow

# COMMAND ----------

# dbutils.widgets.text("env","")

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

bronze_farahflfin_companydimension = f"transformation_{env}.brz_sharepoint_list.vw_farahflfin_companydimension"
bronze_farahflfin_coa_segment_mapping = f"transformation_{env}.brz_sharepoint_list.vw_farahflfin_coa_segment_mapping"

silver_schema = f"transformation_{env}.slv_shared"
silver_table = f"{silver_schema}.prep_dim_company"

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read the bronze table data

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Clean the data from the bronze table and return it as a DataFrame

# COMMAND ----------

cleaned_farahflfin_companydimension_df = clean_and_return_df(bronze_farahflfin_companydimension)
cleaned_farahflfin_coa_segment_mapping_df = clean_and_return_df(bronze_farahflfin_coa_segment_mapping)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join dataframes, then select and transform columns

# COMMAND ----------

from pyspark.sql.functions import col, when, lit

# Join the two dataframes
df_company_silver = cleaned_farahflfin_companydimension_df.alias("company") \
    .join(cleaned_farahflfin_coa_segment_mapping_df.alias("csm"),
          (col("company.company_id") == col("csm.OLD_CODE")) & (col("csm.SEGMENT_NAME") == 'Entity'),
          how="left")

# Select and transform columns
df_company_silver = df_company_silver.select(
    when(col("company.company_code") == "", None).otherwise(col("company.company_code")).alias("COMPANY_CODE"),
    col("company.company_id").alias("COMPANY_ID"),
    when((col("csm.new_code").isNull()) | (col("csm.new_code") == ""), col("company.company_id")) \
        .otherwise(col("csm.new_code")) \
        .alias("COMPANY_ID_NEW"),
    when(col("company.company_name") == "", None).otherwise(col("company.company_name")).alias("COMPANY_NAME"),
    when((col("csm.new_description").isNull()) | (col("csm.new_description") == ""), col("company.company_name")) \
        .otherwise(col("csm.new_description")) \
        .alias("COMPANY_NAME_NEW"),
    col("company.eatec_siteid").alias("EATEC_SITEID"),
    col("company.ig_site_id").alias("IG_SITEID"),
    when(col("company.maximo_siteid") == "", None).otherwise(col("company.maximo_siteid")).alias("MAXIMO_SITEID"),
    col("company.omni_site_id").alias("OMNI_SITEID"),
    col("company.oracle_siteid").alias("ORACLE_SITEID"),
    col("company.parent_company_id").alias("PARENT_COMPANY_ID"),
    col("company.leisure_facility_flag").alias("LEISURE_FACILITY_FLAG"),
    col("company.park_region").alias("PARK_REGION")
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Applying schema `casting`

# COMMAND ----------

from pyspark.sql.functions import col, when, lit

df_company_silver = apply_schema_casting(df_company_silver,dim_company_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Schema Enforcement and then writing final silver table to UC

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["OMNI_SITEID", "IG_SITEID", "COMPANY_ID", "COMPANY_ID_NEW"]

# Find duplicates
duplicate_df = (
    df_company_silver
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in df_company_silver based on keys {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in df_company_silver.")


# COMMAND ----------

# Build DDL string from schema dict
dim_company_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_company_schema.items()
)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_company_schema_ddl)
    if compare_schema_lenient(df_company_silver.schema, ddl_struct):
        print("Schema matches!")
        df_company_silver.write.format("delta")\
                        .option("mergeSchema", True) \
                        .mode("overwrite")\
                        .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        print(ddl_struct)
        print(df_company_silver.schema)
        # displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        # displayHTML(f"<b>Actual:</b><br><pre>{df_company_silver.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise Exception(f"FAIL: {str(e)}")  # Fails the notebook in workflow/task
