# Databricks notebook source
# MAGIC %md
# MAGIC # SUMMARY  
# MAGIC ### Notebook Summary: Silver Layer Table – `sub_analysis`  
# MAGIC This notebook builds the Silver delta table `sub_analysis` using source data from Fusion and SharePoint systems. Below is a summary of the key activities:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. Import Required Libraries  
# MAGIC - Loads essential PySpark functions and types.
# MAGIC
# MAGIC ### 2. Read Workflow Parameters  
# MAGIC - Uses widgets to get `env` and `table_schema` dynamically.
# MAGIC
# MAGIC ### 3. Load Configuration and Utility Functions  
# MAGIC - Executes:  
# MAGIC   - `schema_sil[table_schema]` (for expected schema)  
# MAGIC   - `bias_functions` (for reusable utilities)
# MAGIC
# MAGIC ### 4. Define Source and Target Paths  
# MAGIC - Constructs fully qualified table names for Bronze and Silver layers based on `env`.
# MAGIC
# MAGIC ### 5. Read and Clean Source Tables  
# MAGIC - Reads source tables using `clean_and_return_df()`:
# MAGIC   - `fusion_xxapps_xxflpm_bi_coa_sa_master_all`  
# MAGIC   - `fusion_xxapps_xxflp_omni_dmg_cat_sa_map_all`  
# MAGIC   - `farahflfin_outletmapping`
# MAGIC
# MAGIC ### 6. Build SubAnalysis DataFrame  
# MAGIC - Joins:
# MAGIC   - COA Master + Mapping on `SA_CODE`
# MAGIC   - Includes: DMG info from Mapping
# MAGIC - Applies transformation and null/empty string handling
# MAGIC
# MAGIC ### 7. Build SubAnalysis_IG DataFrame  
# MAGIC - Extracts records without DMG mapping  
# MAGIC - Appends null columns for DMG fields
# MAGIC
# MAGIC ### 8. Combine and Clean Final Data  
# MAGIC - Unions both datasets and removes duplicates  
# MAGIC - Applies `None` handling for empty strings  
# MAGIC - Deduplicates on `SUB_ANALYSIS_CODE`, `DMG_CATEGORY_CODE`
# MAGIC
# MAGIC ### 9. Apply Schema Casting  
# MAGIC - Merges schema definitions and casts columns to expected types
# MAGIC
# MAGIC ### 10. Validate Schema and Write to Silver Table  
# MAGIC - Compares with expected schema DDL  
# MAGIC - If matched, writes delta table in overwrite mode  
# MAGIC - If mismatched, fails with print schema comparison
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols": ["SUB_ANALYSIS_CODE","SUB_ANALYSIS_CODE_OLD"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Required Libraries

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, trim, lower, coalesce,
    regexp_extract, regexp_replace, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

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
# MAGIC ### Define Source and Target Paths for DWH_DIM_SUB_ANALYSIS

# COMMAND ----------

# DBTITLE 1,reading brz
# Define Source and Target Tables for sub_analysis
bronze_schema = f"transformation_{env}.brz_fusion"
sharepoint_schema = f"transformation_{env}.brz_sharepoint_list"  
silver_schema = f"transformation_{env}.slv_shared"

# Corrected bronze table references with vw_ prefix
bronze_coa_sa_master = f"{bronze_schema}.vw_fusion_xxapps_xxflpm_bi_coa_sa_master_all"
bronze_dmg_cat_sa_map = f"{bronze_schema}.vw_fusion_xxapps_xxflp_omni_dmg_cat_sa_map_all"
bronze_outlet_mapping = f"{sharepoint_schema}.vw_farahflfin_outletmapping"
sub_analysis_segment_mapping = f"transformation_{env}.brz_sharepoint_list.vw_farahflfin_coa_segment_mapping"

# Silver output table
silver_table = f"{silver_schema}.prep_dim_sub_analysis"

# COMMAND ----------

# MAGIC %md
# MAGIC ### COA SA Master extraction 

# COMMAND ----------

# 1. Read COA Master table (oraclefusion)
coa_sa_master_df = clean_and_return_df(bronze_coa_sa_master)

# Select only the columns we need based on extraction query
coa_sa_master_df = coa_sa_master_df.select(
    "SA_CODE", 
    "SA_NAME", 
    "SUMMARY_FLAG", 
    "LAST_UPDATE_DATE", 
    "LAST_UDPATED_BY"  # Note: keeping the typo as it exists in source
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DMG Category SA Map extraction

# COMMAND ----------

# 2. Read DMG Category SA Map table
dmg_cat_sa_map_df = clean_and_return_df(bronze_dmg_cat_sa_map)

# Select the columns we need based on extraction query
dmg_cat_sa_map_df = dmg_cat_sa_map_df.select(
    "SITE",
    "DMG_CATEGORY_CODE",
    "DMG_CATEGORY_NAME",
    "DMG_CATEGORY_DESC",
    "SUB_ANALYSIS_CODE",
    "SUB_ANALYSIS_DESC",
    "ORACLE_CUST_IMPORT",
    "ACTIVE_FLAG",
    "ATTRIBUTE1",
    "ATTRIBUTE2",
    "ATTRIBUTE3",
    "ATTRIBUTE4",
    "ATTRIBUTE5",
    "CREATION_DATE",
    "CREATED_BY",
    "LAST_UPDATE_DATE",
    "LAST_UPDATED_BY"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OutletMapping SharePoint

# COMMAND ----------

# 3. Read Outlet Mapping table (SharePoint)
outlet_mapping_df = clean_and_return_df(bronze_outlet_mapping)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read dwh_dim_sub_analysis from archive for _old columns

# COMMAND ----------

sub_analysis_segment_mapping_df = clean_and_return_df(sub_analysis_segment_mapping)

sub_analysis_segment_mapping_df = sub_analysis_segment_mapping_df.filter(col("segment_name") == "Sub Analysis")


# Select only the columns we need for OLD data
sub_analysis_segment_mapping_df = sub_analysis_segment_mapping_df.select(
    col("NEW_CODE").alias("SUB_ANALYSIS_CODE"),
    col("OLD_CODE").alias("SUB_ANALYSIS_CODE_OLD"),
    col("OLD_DESCRIPTION").alias("SUB_ANALYSIS_NAME_OLD")
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Construct SubAnalysis Variant by Joining COA Master with Outlet Mapping and DMG Category Mapping 

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, when

# Step 1: Perform the joins
joined_df = (
    coa_sa_master_df.alias("coa_master_all")
    .join(
        outlet_mapping_df.alias("sp_out"),
        col("coa_master_all.SA_CODE") == col("sp_out.OracleOutletCode"),
        "left_outer"
    )
    .join(
        dmg_cat_sa_map_df.alias("coa_map_all"),
        col("coa_master_all.SA_CODE") == col("coa_map_all.SUB_ANALYSIS_CODE"),
        "left"
    )
)

# Step 2: Select and transform columns
selected_df = (
    joined_df
    .select(
        col("coa_master_all.SA_CODE").alias("SUB_ANALYSIS_CODE"),
        col("coa_master_all.SA_NAME").alias("SUB_ANALYSIS_NAME"),
        col("coa_master_all.SUMMARY_FLAG").alias("SUMMARY_FLAG"),
        lit(None).cast("string").alias("REVENUE_STREAM")
    )
    .distinct()
)

# Step 3: Replace empty strings with NULLs
final_df = (
    selected_df
    .withColumn("SUB_ANALYSIS_CODE", when(col("SUB_ANALYSIS_CODE") == "", lit(None)).otherwise(col("SUB_ANALYSIS_CODE")))
    .withColumn("SUB_ANALYSIS_NAME", when(col("SUB_ANALYSIS_NAME") == "", lit(None)).otherwise(col("SUB_ANALYSIS_NAME")))
    .withColumn("SUMMARY_FLAG", when(col("SUMMARY_FLAG") == "", lit(None)).otherwise(col("SUMMARY_FLAG")))
    .withColumn("REVENUE_STREAM", when(col("REVENUE_STREAM") == "", lit(None)).otherwise(col("REVENUE_STREAM")))
    .withColumn("SUB_ANALYSIS_CODE_OLD", lit(None).cast("string"))
    .withColumn("SUB_ANALYSIS_NAME_OLD", lit(None).cast("string")) 
)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Join with Snowflake Archive to Add OLD Columns

# COMMAND ----------

from pyspark.sql import functions as F, Window

final_df_enhanced = (
    final_df.alias("sub_analysis")
    .join(
        sub_analysis_segment_mapping_df.alias("sub_analysis_old"),
        F.col("sub_analysis.SUB_ANALYSIS_CODE") == F.col("sub_analysis_old.SUB_ANALYSIS_CODE"),
        "left"
    )
    .select(
        F.col("sub_analysis.SUB_ANALYSIS_CODE").alias("SUB_ANALYSIS_CODE"),
        F.col("sub_analysis_old.SUB_ANALYSIS_CODE_OLD").alias("SUB_ANALYSIS_CODE_OLD"),        
        F.col("sub_analysis.SUB_ANALYSIS_NAME").alias("SUB_ANALYSIS_NAME"),
        F.col("sub_analysis.SUMMARY_FLAG").alias("SUMMARY_FLAG"),
        F.col("sub_analysis.REVENUE_STREAM").alias("REVENUE_STREAM"),
        F.col("sub_analysis_old.SUB_ANALYSIS_NAME_OLD").alias("SUB_ANALYSIS_NAME_OLD")
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Schema Casting

# COMMAND ----------

from pyspark.sql.functions import col, when

final_df_enhanced = final_df_enhanced.select([
    when(col(c) == '', None).otherwise(col(c)).alias(c)
    for c in final_df_enhanced.columns
])


final_df_enhanced = apply_schema_casting(final_df_enhanced, dim_sub_analysis_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Schema and Write to Silver Table

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["SUB_ANALYSIS_CODE", "SUB_ANALYSIS_CODE_OLD"]

# Find duplicates
duplicate_df = (
    final_df_enhanced
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in final_df_enhanced based on key {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in final_df_enhanced.")


# COMMAND ----------

# Build DDL string from schema dict
dim_sub_analysis_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_sub_analysis_schema.items()
)

# print(dim_sub_analysis_schema_ddl)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_sub_analysis_schema_ddl)
    if compare_schema_lenient(final_df_enhanced.schema, ddl_struct):
        print("Schema matches!")
        final_df_enhanced.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{final_df_enhanced.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e
