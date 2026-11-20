# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC
# MAGIC **Notebook Summary: Silver Layer Table - `gl_account`**
# MAGIC
# MAGIC This notebook builds the silver Delta table `gl_account` using source data from Fusion and SharePoint systems. Below is a summary of the key activities:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. Import Required Libraries
# MAGIC - Loads essential PySpark functions and types.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Read Workflow Parameters
# MAGIC - Uses widgets to get `env` and `table_schema` dynamically.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Load Configuration and Utility Functions
# MAGIC - Executes:
# MAGIC   - `schema_config` (for expected schemas)
# MAGIC   - `base_functions` (for reusable utilities)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Define Source and Target Paths
# MAGIC - Constructs fully qualified table names for Bronze and Silver layers based on `env`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. Read and Clean Source Tables
# MAGIC - Reads 4 source tables using `clean_and_return_df()`:
# MAGIC   - `coa_acct_master_all`
# MAGIC   - `acct_map_hdr_all`
# MAGIC   - `acct_map_dtl_all`
# MAGIC   - `glgroupmapping` (SharePoint)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. Create Staging View
# MAGIC - Joins account map detail and header tables to prepare `GL_ACCOUNT_STAGING`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 7. Build Final DataFrame
# MAGIC - Joins:
# MAGIC   - COA Master + Staging on `ACCT_CODE`
# MAGIC   - Result + GL Group Mapping
# MAGIC - Applies transformations and null-safe column handling.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 8. Select Final Output Columns
# MAGIC - Keeps only the relevant business columns (excludes `OLD_CODE`, etc.)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 9. Apply Schema Casting
# MAGIC - Merges source schemas and casts columns to expected types.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 10. Validate Schema and Write to Silver Table
# MAGIC - Compares schema with expected DDL.
# MAGIC - If matched, writes to Delta table with overwrite mode.
# MAGIC - If mismatch, notebook fails and prints schema comparison.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols": ["GL_ACCOUNT_CODE", "GL_ACCOUNT_CODE_OLD"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Required Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, NumericType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, initcap, concat_ws

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Workflow Parameters

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
# MAGIC ### Define Source and Target Paths for DWH_DIM_GL_ACCOUNT

# COMMAND ----------

# Define Source and Target Tables using vw_ views
bronze_schema = f"transformation_{env}.brz_fusion"
sharepoint_schema = f"transformation_{env}.brz_sharepoint_list"
silver_schema = f"transformation_{env}.slv_shared"

bronze_coa_master = f"{bronze_schema}.vw_fusion_xxapps_xxflpm_bi_coa_acct_master_all"
bronze_coa_mapping_segment = f"{sharepoint_schema}.vw_farahflfin_coa_segment_mapping"
bronze_acct_map_hdr = f"{bronze_schema}.vw_fusion_xxapps_xxflpm_bi_acct_map_hdr_all"
bronze_acct_map_dtl = f"{bronze_schema}.vw_fusion_xxapps_xxflpm_bi_acct_map_dtl_all"
bronze_glgroupmapping = f"{sharepoint_schema}.vw_farahflfin_glgroupmapping"

silver_table = f"{silver_schema}.prep_dim_gl_account"

print(f"bronze_schema           = {bronze_schema}")
print(f"sharepoint_schema       = {sharepoint_schema}")
print(f"silver_schema           = {silver_schema}")
print(f"bronze_coa_master       = {bronze_coa_master}")
print(f"bronze_acct_map_hdr     = {bronze_acct_map_hdr}")
print(f"bronze_acct_map_dtl     = {bronze_acct_map_dtl}")
print(f"bronze_glgroupmapping   = {bronze_glgroupmapping}")
print(f"silver_table            = {silver_table}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read and Clean Source Tables

# COMMAND ----------

# Read and clean source dataframes
coa_master_df = clean_and_return_df(bronze_coa_master)
coa_mapping_seg_df = clean_and_return_df(bronze_coa_mapping_segment)
acct_map_hdr_df = clean_and_return_df(bronze_acct_map_hdr)
acct_map_dtl_df = clean_and_return_df(bronze_acct_map_dtl)
glgroupmapping_df = clean_and_return_df(bronze_glgroupmapping)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Staging View

# COMMAND ----------

acct_map_dtl_df_cols = ["HDR_ID", "ACCT_CODE", "LAST_UPDATE_DATE", "LAST_UPDATED_BY"]
acct_map_dtl_df.select(acct_map_dtl_df_cols).createOrReplaceTempView("acct_map_dtl")

coa_master_df_cols = ["ACCT_CODE", expr("replace(ACCT_NAME, 'û', '-')").alias("ACCT_NAME"), "ACCT_TYPE", "SUMMARY_FLAG", "LAST_UPDATE_DATE", "LAST_UDPATED_BY"]
coa_master_df.select(*coa_master_df_cols).createOrReplaceTempView("coa_acct_master_all")

coa_mapping_seg_df.filter("SEGMENT_NAME = 'Account'").createOrReplaceTempView("COA_MAPPING_SEGMENT")

acct_map_hdr_df_cols = ["HDR_ID", "SOURCE", "MAIN_TYPE", "SUB_TYPE", "EBITDA_FLAG", "CREATION_DATE", "CREATED_BY", "LAST_UPDATE_DATE", "LAST_UPDATED_BY", "EBITDA_SUB_CATEGORY"]
acct_map_hdr_df.select(acct_map_hdr_df_cols).createOrReplaceTempView("acct_map_hdr")

glgroupmapping_df.createOrReplaceTempView("GLGroupMapping")

# COMMAND ----------

# %sql
# select * from coa_acct_master_all where ACCT_NAME like '%DMD Marketing ? Yas Bay Waterfront%';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Staging dataframe

# COMMAND ----------

query = f"""
SELECT 
  hdr.HDR_ID AS HDR_ID,
  hdr.SOURCE AS SOURCE,
  hdr.MAIN_TYPE AS MAIN_TYPE,
  hdr.SUB_TYPE AS SUB_TYPE,
  hdr.EBITDA_FLAG AS EBITDA_FLAG,
  hdr.CREATION_DATE AS CREATION_DATE,
  hdr.CREATED_BY AS CREATED_BY,
  hdr.LAST_UPDATE_DATE AS LAST_UPDATE_DATE,
  hdr.LAST_UPDATED_BY AS LAST_UPDATED_BY,
  hdr.EBITDA_SUB_CATEGORY AS EBITDA_SUB_CATEGORY,
  dtl.ACCT_CODE AS ACCT_CODE
FROM acct_map_dtl dtl
LEFT OUTER JOIN acct_map_hdr hdr
  ON dtl.HDR_ID = hdr.HDR_ID;
"""
staging_df = spark.sql(query)
staging_df.createOrReplaceTempView("GL_ACCOUNT_STAGING")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Final DataFrame

# COMMAND ----------

outquery = f"""
SELECT
  CASE WHEN GL_GROUP_NAME = '' THEN NULL ELSE GL_GROUP_NAME END AS GL_GROUP_NAME,
  CASE WHEN GL_ACCOUNT_CODE = '' THEN NULL ELSE GL_ACCOUNT_CODE END AS GL_ACCOUNT_CODE,
  CASE WHEN GL_ACCOUNT_NAME = '' THEN NULL ELSE REPLACE(GL_ACCOUNT_NAME, '?', '-') END AS GL_ACCOUNT_NAME,
  CASE WHEN GL_GROUP_EFFECTIVE_FROM = '' THEN NULL ELSE GL_GROUP_EFFECTIVE_FROM END AS GL_GROUP_EFFECTIVE_FROM,
  CASE WHEN GL_GROUP_EFFECTIVE_TO = '' THEN NULL ELSE GL_GROUP_EFFECTIVE_TO END AS GL_GROUP_EFFECTIVE_TO,
  CASE WHEN GL_ACCOUNT_TYPE = '' THEN NULL ELSE GL_ACCOUNT_TYPE END AS GL_ACCOUNT_TYPE,
  CASE WHEN GL_ACCOUNT_TYPE_NAME = '' THEN NULL ELSE GL_ACCOUNT_TYPE_NAME END AS GL_ACCOUNT_TYPE_NAME,
  CASE WHEN SUMMAY_FLAG = '' THEN NULL ELSE SUMMAY_FLAG END AS SUMMAY_FLAG,
  CASE WHEN ACCOUNT_SUB_TYPE_NAME = '' THEN NULL ELSE ACCOUNT_SUB_TYPE_NAME END AS ACCOUNT_SUB_TYPE_NAME,
  CASE WHEN ACCOUNT_SUB_TYPE_CODE = '' THEN NULL ELSE ACCOUNT_SUB_TYPE_CODE END AS ACCOUNT_SUB_TYPE_CODE,
  CASE WHEN EBITDA_FLAG = '' THEN NULL ELSE EBITDA_FLAG END AS EBITDA_FLAG,
  CASE WHEN EBITDA_SUB_CATEGORY = '' THEN NULL ELSE EBITDA_SUB_CATEGORY END AS EBITDA_SUB_CATEGORY,
  MODIFIED_DATETIME,
  CREATED_DATETIME,
  CASE WHEN CURRENT_FLAG = '' THEN NULL ELSE CURRENT_FLAG END AS CURRENT_FLAG,
  CASE WHEN GL_ACCOUNT_CODE_OLD = '' THEN NULL ELSE GL_ACCOUNT_CODE_OLD END AS GL_ACCOUNT_CODE_OLD,
  CASE WHEN GL_ACCOUNT_NAME_OLD = '' THEN NULL ELSE GL_ACCOUNT_NAME_OLD END AS GL_ACCOUNT_NAME_OLD
FROM (
  SELECT DISTINCT
    gl_group.GLGroup AS GL_GROUP_NAME,
    coa_acct_master_all.ACCT_CODE AS GL_ACCOUNT_CODE,
    coa_acct_master_all.ACCT_NAME AS GL_ACCOUNT_NAME,
    COALESCE(gl_group.EffectiveFromDate, TIMESTAMP('1900-01-01')) AS GL_GROUP_EFFECTIVE_FROM,
    CAST(gl_group.EffectiveUntilDate AS TIMESTAMP) AS GL_GROUP_EFFECTIVE_TO,
    gl_group.AccountType AS GL_ACCOUNT_TYPE,
    out1.SOURCE AS GL_ACCOUNT_TYPE_NAME,
    coa_acct_master_all.SUMMARY_FLAG AS SUMMAY_FLAG,
    out1.MAIN_TYPE AS ACCOUNT_SUB_TYPE_NAME,
    out1.SUB_TYPE AS ACCOUNT_SUB_TYPE_CODE,
    out1.EBITDA_FLAG AS EBITDA_FLAG,
    out1.EBITDA_SUB_CATEGORY AS EBITDA_SUB_CATEGORY,
    current_timestamp() AS MODIFIED_DATETIME,
    current_timestamp() AS CREATED_DATETIME,
    '' AS CURRENT_FLAG,
    CMS.OLD_CODE AS GL_ACCOUNT_CODE_OLD,
    CMS.OLD_DESCRIPTION AS GL_ACCOUNT_NAME_OLD
  FROM coa_acct_master_all
  LEFT JOIN GL_ACCOUNT_STAGING out1
    ON out1.ACCT_CODE = coa_acct_master_all.ACCT_CODE
  LEFT JOIN GLGroupMapping gl_group
    ON coa_acct_master_all.ACCT_CODE = gl_group.GLAccountCode
  LEFT JOIN COA_MAPPING_SEGMENT CMS
    ON coa_acct_master_all.ACCT_CODE = CMS.NEW_CODE
)
"""
final_df = spark.sql(outquery)


# COMMAND ----------

# final_df.select("GL_ACCOUNT_NAME").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Final Output Columns

# COMMAND ----------

# Select required columns without OLD_CODE and OLD_DESCRIPTION
required_columns = [
    "GL_ACCOUNT_CODE",
    "GL_ACCOUNT_CODE_OLD",
    "GL_GROUP_NAME",
    "GL_ACCOUNT_NAME",
    "GL_GROUP_EFFECTIVE_FROM",
    "GL_GROUP_EFFECTIVE_TO",
    "GL_ACCOUNT_TYPE",
    "GL_ACCOUNT_TYPE_NAME",
    "SUMMAY_FLAG",
    "ACCOUNT_SUB_TYPE_NAME",
    "ACCOUNT_SUB_TYPE_CODE",
    "EBITDA_FLAG",
    "EBITDA_SUB_CATEGORY",
    "GL_ACCOUNT_NAME_OLD"
]

gl_account_df = final_df.select(*required_columns)


# COMMAND ----------

# gl_account_df.select("GL_ACCOUNT_NAME").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Schema Casting

# COMMAND ----------

gl_account_df = gl_account_df.dropDuplicates(["GL_ACCOUNT_CODE", "GL_ACCOUNT_CODE_OLD"])

from pyspark.sql.functions import col, when

gl_account_df = gl_account_df.select([
    when(col(c) == '', None).otherwise(col(c)).alias(c)
    for c in gl_account_df.columns
])


# Apply casting to the final DataFrame using the merged schema
gl_account_df = apply_schema_casting(gl_account_df, dim_gl_account_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Schema and Write to Silver Table

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["GL_ACCOUNT_CODE", "GL_ACCOUNT_CODE_OLD"]

# Find duplicates
duplicate_df = (
    gl_account_df
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in gl_account_df based on keys {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in gl_account_df.")


# COMMAND ----------

# Build DDL string from schema dict
dim_gl_account_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_gl_account_schema.items()
)

# print(dim_gl_account_schema_ddl)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_gl_account_schema_ddl)
    if compare_schema_lenient(gl_account_df.schema, ddl_struct):
        print("Schema matches!")
        gl_account_df.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{gl_account_df.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e
