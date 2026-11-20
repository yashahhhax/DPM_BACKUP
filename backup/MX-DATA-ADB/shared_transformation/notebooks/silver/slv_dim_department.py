# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC 1. Source table:
# MAGIC **vw_fusion_xxapps_xxflpm_bi_coa_dept_master_all**,
# MAGIC
# MAGIC
# MAGIC 2. Target silver table: **prep_dim_department**
# MAGIC
# MAGIC 3. Schema enforcement implemented 

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols": ["DEPARTMENT_CODE","DEPARTMENT_CODE_OLD"]

# COMMAND ----------

# DBTITLE 1,fetch widgets
# dbutils.widgets.text("env","")

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,loading schema_config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# MAGIC %md
# MAGIC ##Contains all the base functions required

# COMMAND ----------

# DBTITLE 1,loading base funcs
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##Base function call to receive the param values

# COMMAND ----------

try:
    notebook_vars = [
        'env'
    ]
    env= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# print(env)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Source and Target Tables

# COMMAND ----------

# DBTITLE 1,defining source tables

# Define the Source bronze table names

brz_xxflpm_bi_coa_dept_master_all=f"transformation_{env}.brz_fusion.vw_fusion_xxapps_xxflpm_bi_coa_dept_master_all"
#brz_xxflp_bi_coa_segment_values_all=f"transformation_{env}.brz_fusion.vw_fusion_xxapps_xxflp_bi_coa_segment_values_all"
dept_segment_mapping = f"transformation_{env}.brz_sharepoint_list.vw_farahflfin_coa_segment_mapping"
slv_schema = f"transformation_{env}.slv_shared"
slv_table = f"{slv_schema}.prep_dim_department"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning of source data 

# COMMAND ----------

# Clean source data
src_xxflpm_bi_coa_dept_master_all_df = clean_and_return_df(brz_xxflpm_bi_coa_dept_master_all)
dept_segment_mapping_df  = clean_and_return_df(dept_segment_mapping)

# COMMAND ----------

# DBTITLE 1,selecting only active records
dept_segment_mapping_df = dept_segment_mapping_df.filter(col("segment_name") == "Dept")

# COMMAND ----------

# MAGIC %md ## Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read required columns & apply filter logic

# COMMAND ----------

# Create cleaned and selected DataFrame
bi_coa_dept_master_all_df = (
    src_xxflpm_bi_coa_dept_master_all_df
    .select(
        "DEPT_CODE",
        "DEPT_NAME",
        "OPERATIONAL_FLAG",
        "SUMMARY_FLAG",
        "LAST_UPDATE_DATE",
        "LAST_UDPATED_BY"
    )
)

# COMMAND ----------

# Select and rename columns, ensure distinct rows
from pyspark.sql.functions import lit
bi_coa_dept_master_all_df = bi_coa_dept_master_all_df.select(
    col("DEPT_CODE").alias("DEPARTMENT_CODE"),
    col("DEPT_NAME").alias("DEPARTMENT_NAME"),
    col("OPERATIONAL_FLAG")
).distinct()

# Replace empty strings with nulls
columns_to_clean = ["DEPARTMENT_CODE", "DEPARTMENT_NAME", "OPERATIONAL_FLAG"]
for column_name in columns_to_clean:
    df_final = bi_coa_dept_master_all_df.withColumn(column_name, replace_empty_string_with_null(column_name))


# COMMAND ----------

dept_segment_mapping_df = clean_and_return_df(dept_segment_mapping)

dept_segment_mapping_df = dept_segment_mapping_df.filter(col("segment_name") == "Dept")


# Select only the columns we need for OLD data
dept_segment_mapping_df = dept_segment_mapping_df.select(
    col("NEW_CODE").alias("DEPARTMENT_CODE"),
    col("OLD_CODE").alias("DEPARTMENT_CODE_OLD"),
    col("OLD_DESCRIPTION").alias("DEPARTMENT_NAME_OLD")
).distinct()

# COMMAND ----------


dept_segment_mapping_df1 = (
    dept_segment_mapping_df
    .select(
        col("DEPARTMENT_CODE"),
        col("DEPARTMENT_CODE_OLD"),
        col("DEPARTMENT_NAME_OLD")
    )
    )
# columns to clean (string columns)
columns_to_clean = [
    "DEPARTMENT_CODE",
    "DEPARTMENT_CODE_OLD",
    "DEPARTMENT_NAME_OLD"
]
# start from df and progressively overwrite df_final
dept_segment_mapping_df2 = dept_segment_mapping_df1

for column_name in columns_to_clean:
    dept_segment_mapping_df2 = dept_segment_mapping_df2.withColumn(
        column_name, replace_empty_string_with_null(column_name)
    )


# COMMAND ----------

df_final = df_final.withColumn("DEPARTMENT_CODE_OLD", lit(None).cast("string")) \
                   .withColumn("DEPARTMENT_NAME_OLD", lit(None).cast("string"))

# COMMAND ----------

from pyspark.sql.functions import col, coalesce

load_df = (
    df_final.alias("t1").join(dept_segment_mapping_df2.alias("t2"), col("t1.DEPARTMENT_CODE") == col("t2.DEPARTMENT_CODE"),"left")
    .select(
        col("t1.DEPARTMENT_CODE").alias("DEPARTMENT_CODE"),
        col("t1.DEPARTMENT_NAME").alias("DEPARTMENT_NAME"),
        col("t1.OPERATIONAL_FLAG").alias("OPERATIONAL_FLAG"),
        col("t2.DEPARTMENT_CODE_OLD").alias("DEPARTMENT_CODE_OLD"),
        col("t2.DEPARTMENT_NAME_OLD").alias("DEPARTMENT_NAME_OLD")
    )
    .distinct()
)


# COMMAND ----------

from pyspark.sql.functions import col, when

load_df = load_df.select([
    when(col(c) == '', None).otherwise(col(c)).alias(c)
    for c in load_df.columns
])

department_df = apply_schema_casting(load_df, dim_department_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Schema Enforcement and writing final silver table to UC

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["DEPARTMENT_CODE", "DEPARTMENT_CODE_OLD"]

# Find duplicates
duplicate_df = (
    department_df
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in department_df based on keys {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in department_df.")


# COMMAND ----------

# Build DDL string from schema dict
dim_department_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_department_schema.items()
)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_department_schema_ddl)
    if compare_schema_lenient(department_df.schema, ddl_struct):
        print("Schema matches!")
        department_df.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .saveAsTable(slv_table)
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{department_df.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e
