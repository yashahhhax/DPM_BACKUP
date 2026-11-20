# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC 1. Source tables: \
# MAGIC **vw_fusion_xxapps_xxflpm_bi_emp_mast_all**,\
# MAGIC **vw_fusion_xxapps_xxflpm_bi_dept_master_all**,\
# MAGIC **vw_fusion_xxapps_xxflpm_bi_emp_service_dtls_all**,\
# MAGIC **vw_fusion_xxapps_xxflpm_bi_people_dtls_all**,\
# MAGIC **vw_fusion_xxapps_xxflpm_bi_position_master_all**
# MAGIC
# MAGIC 2. Target silver table: **prep_dim_employee**
# MAGIC
# MAGIC 3. Schema enforcement implemented 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols":["EMPLOYEE_NUMBER","EMPLOYEE_ID"]

# COMMAND ----------

# Import required functions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType

# COMMAND ----------

# dbutils.widgets.text("env","")

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %run ../../config/schema_config

# COMMAND ----------

# MAGIC %md
# MAGIC ##Contains all the base functions required

# COMMAND ----------

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


# Define the bronze table names

brz_xxflpm_bi_emp_mast_all=f"transformation_{env}.brz_fusion.vw_fusion_xxapps_xxflpm_bi_emp_mast_all"
brz_xxflpm_bi_dept_master_all=f"transformation_{env}.brz_fusion.vw_fusion_xxapps_xxflpm_bi_dept_master_all"
brz_xxflpm_bi_emp_service_dtls_all=f"transformation_{env}.brz_fusion.vw_fusion_xxapps_xxflpm_bi_emp_service_dtls_all"
brz_xxflpm_bi_people_dtls_all=f"transformation_{env}.brz_fusion.vw_fusion_xxapps_xxflpm_bi_people_dtls_all"
brz_xxflpm_bi_position_master_all=f"transformation_{env}.brz_fusion.vw_fusion_xxapps_xxflpm_bi_position_master_all"

# Define the silver schema and table name
slv_schema = f"transformation_{env}.slv_shared"
slv_table = f"{slv_schema}.prep_dim_employee"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning of source data 

# COMMAND ----------

# Clean source data
src_xxflpm_bi_emp_mast_all_df = clean_and_return_df(brz_xxflpm_bi_emp_mast_all)
src_xxflpm_bi_dept_master_all_df = clean_and_return_df(brz_xxflpm_bi_dept_master_all)
src_xxflpm_bi_emp_service_dtls_all_df = clean_and_return_df(brz_xxflpm_bi_emp_service_dtls_all)
src_xxflpm_bi_people_dtls_all_df = clean_and_return_df(brz_xxflpm_bi_people_dtls_all)
src_xxflpm_bi_position_master_all_df = clean_and_return_df(brz_xxflpm_bi_position_master_all)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read required columns & apply filter logic

# COMMAND ----------

# MAGIC %md #####src_bi_emp_mast_all

# COMMAND ----------

# Select filtered data:

emp_mast= src_xxflpm_bi_emp_mast_all_df.select(
        "COMPANY_CODE",
        "EMPLOYEE_ID",
        "FIRST_NAME",
        "LAST_NAME",
        "GENDER",
        "DATE_OF_BIRTH",
        "NATIONALITY_ID",
        "EMIRATES_ID",
        "OFFER_DATE",
        "POSTING_DATE",
        "JOINED_DATE",
        "EMPLOYEE_LAST_DATE",
        "EMPLOYEE_TYPE_ID",
        "POSITION_ID",
        "DEPARTMENT_ID",
        "TURNOVER_ID",
        "LAST_UPDATE_DATE",
        "LAST_UDPATED_BY",
        "EMPLOYEE_NUMBER",
        "ACCOMMODATION_TYPE",
        "NATIONALITY_CATEGORY",
        "POSITION_START_DATE",
        "LEAVERS_CATEGORY",
        "EMP_LOCATION",
        "WORK_SCHEDULE",
        "EMP_STATUS",
        "POINT_OF_ORIGIN",
        "POINT_OF_ORIGIN_CODE",
        "EMP_CATEGORY",
    )

# COMMAND ----------

# MAGIC %md #####src_bi_dept_master_all

# COMMAND ----------

# Select respective columns & apply filter logic:
bi_dept_master_all = (
    src_xxflpm_bi_dept_master_all_df.select(
        "COMPANY_CODE",
        "DEPARTMENT_ID",
        "DEPARTMENT_CODE",
        "DEPARTMENT_DESC",
        "LAST_UDPATED_BY",
        "LAST_UPDATE_DATE",
        "SITE"
    )   
)

dept_raw = bi_dept_master_all.filter(F.col("DEPARTMENT_CODE").isNotNull())

# COMMAND ----------

# MAGIC %md #####src_bi_emp_service_dtls_all

# COMMAND ----------

# Apply filter logic & select columns :

filtered_df = src_xxflpm_bi_emp_service_dtls_all_df.select(
    "PERIOD_OF_SERVICE_ID",
    "PERSON_ID",
    "DATE_START",
    "ACTUAL_TERMINATION_DATE",
    "FINAL_PROCESS_DATE",
    "LAST_STANDARD_PROCESS_DATE",
    "LEAVING_REASON",
    "LEAVING_REASON_CODE",
    "LAST_UPDATE_DATE",
    "RESIGNATION_DATE",
    "LAST_PAYMENT_DATE",
    "PROJECTED_LAST_DAY",
)

service_raw = filtered_df.filter(F.col("PERIOD_OF_SERVICE_ID").isNotNull())

# COMMAND ----------

# MAGIC %md #####src_bi_people_dtls_all

# COMMAND ----------

# Apply filter logic & select columns :

filtered_df = src_xxflpm_bi_people_dtls_all_df.select(
    "PERSON_ID",
    "EMPLOYEE_NUMBER",
    "FULL_NAME",
    "EMAIL_ADDRESS",
    "PERSON_TYPE_ID",
    "ORIGINAL_DATE_OF_HIRE",
    "NATIONALITY",
    "CURRENT_EMPLOYEE_FLAG",
    "DATE_OF_BIRTH",
    "MARITAL_STATUS",
    "SEX",
    "EFFECTIVE_START_DATE",
    "EFFECTIVE_END_DATE",
    "CREATION_DATE",
    "LAST_UPDATE_DATE",
)


people_raw = filtered_df.filter(F.col("PERSON_ID").isNotNull())

# COMMAND ----------

# MAGIC %md #####src_bi_position_master_all

# COMMAND ----------

bi_position_master_all = src_xxflpm_bi_position_master_all_df.alias("position")
position_raw = bi_position_master_all.filter(F.col("POSITION_ID").isNotNull())

# COMMAND ----------

# MAGIC %md ## Transformations

# COMMAND ----------

# MAGIC %run "../../functions/drr_functions"

# COMMAND ----------

# Create row_number for dept
grouped_df_dept = dept_raw.select("DEPARTMENT_CODE", "DEPARTMENT_DESC", "DEPARTMENT_ID").distinct()
dept_window = Window.partitionBy("DEPARTMENT_CODE", "DEPARTMENT_DESC", "DEPARTMENT_ID").orderBy(F.col("DEPARTMENT_CODE").desc())
dept = grouped_df_dept.withColumn("TT", F.row_number().over(dept_window))

# Create row_number for service
grouped_df_service = service_raw.select(
    "PERIOD_OF_SERVICE_ID",
    "ACTUAL_TERMINATION_DATE",
    "FINAL_PROCESS_DATE",
    "LAST_STANDARD_PROCESS_DATE",
    "RESIGNATION_DATE",
    "LAST_PAYMENT_DATE",
    "PROJECTED_LAST_DAY",
    "LEAVING_REASON_CODE",
    "LEAVING_REASON",
).distinct()

service_window = Window.partitionBy("PERIOD_OF_SERVICE_ID").orderBy(
    F.col("PERIOD_OF_SERVICE_ID").desc()
)
service = grouped_df_service.withColumn("TT", F.row_number().over(service_window))

# Create row_number for people
grouped_df_people = people_raw.select(
    "EMAIL_ADDRESS", "NATIONALITY", "CURRENT_EMPLOYEE_FLAG", "PERSON_ID"
).distinct()
people_window = Window.partitionBy("PERSON_ID").orderBy(F.col("PERSON_ID").desc())
people = grouped_df_people.withColumn("TT", F.row_number().over(people_window))

#Create row_number for position
grouped_df_position = position_raw.select(
    "POSITION_ID"
).distinct()
position_window = Window.partitionBy("POSITION_ID").orderBy(F.col("POSITION_ID").desc())
position = grouped_df_position.withColumn("TT", F.row_number().over(position_window))

# Join the tables
df = (
    emp_mast.alias("emp")
    .join(
        dept.alias("dept"),
        (F.col("emp.DEPARTMENT_ID") == F.col("dept.DEPARTMENT_ID"))
        & (F.col("dept.TT") == 1),
        "left_outer",
    )
    .join(
        service.alias("service"),
        (F.col("emp.EMPLOYEE_ID") == F.col("service.PERIOD_OF_SERVICE_ID"))
        & (F.col("service.TT") == 1),
        "left_outer",
    )
    .join(
        people.alias("people"),
        (F.col("emp.EMPLOYEE_ID") == F.col("people.PERSON_ID"))
        & (F.col("people.TT") == 1),
        "left_outer"
    )
    #New join
    .join(
        position.alias("position"),
        (F.col("emp.POSITION_ID") == F.col("position.POSITION_ID"))
        & (F.col("position.TT") == 1),
        "left_outer",
    )
)

# Apply row_number() to keep only latest record
window_spec = Window.partitionBy("emp.EMPLOYEE_NUMBER", "emp.EMPLOYEE_ID").orderBy(
    F.col("emp.POSITION_START_DATE").cast("timestamp").desc()
)
df = df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1)

# Select and transform all columns using logic
df = df.select(
    replace_empty_string_with_null("emp.EMPLOYEE_NUMBER")
    .cast("bigint")
    .alias("EMPLOYEE_NUMBER"),
    replace_empty_string_with_null("emp.EMPLOYEE_ID")
    .cast("bigint")
    .alias("EMPLOYEE_ID"),
    F.when(
        F.concat_ws(" ", F.col("emp.FIRST_NAME"), F.col("emp.LAST_NAME")) == "", None
    )
    .otherwise(F.concat_ws(" ", F.col("emp.FIRST_NAME"), F.col("emp.LAST_NAME")))
    .alias("EMPLOYEE_NAME"),
    replace_empty_string_with_null("people.EMAIL_ADDRESS").alias("EMPLOYEE_EMAIL"),
    replace_empty_string_with_null("dept.DEPARTMENT_DESC").alias("EMPLOYEE_DEPARTMENT"),
    replace_empty_string_with_null("emp.GENDER").alias("GENDER"),
    replace_empty_string_with_null("people.NATIONALITY").alias("NATIONALITY"),
    F.when(
        F.col("people.CURRENT_EMPLOYEE_FLAG").isNull()
        | (F.col("people.CURRENT_EMPLOYEE_FLAG") == ""),
        "N",
    )
    .otherwise(F.col("people.CURRENT_EMPLOYEE_FLAG"))
    .alias("CURRENT_EMPLOYEE_FLAG"),
    F.lit("PERMANENT EMPLOYEE").alias("EMPLOYEE_ROLE"),
    replace_empty_string_with_null("emp.FIRST_NAME").alias("FIRST_NAME"),
    replace_empty_string_with_null("emp.LAST_NAME").alias("LAST_NAME"),
    replace_empty_string_with_null("emp.DATE_OF_BIRTH")
    .cast("timestamp")
    .alias("BIRTH_DATE"),
    replace_empty_string_with_null("emp.EMIRATES_ID").alias("EMIRATES_ID"),
    replace_empty_string_with_null("emp.OFFER_DATE")
    .cast("timestamp")
    .alias("OFFER_DATE"),
    replace_empty_string_with_null("emp.POSTING_DATE")
    .cast("timestamp")
    .alias("POSTING_DATE"),
    replace_empty_string_with_null("emp.JOINED_DATE")
    .cast("timestamp")
    .alias("JOINED_DATE"),
    replace_empty_string_with_null("emp.EMPLOYEE_LAST_DATE")
    .cast("timestamp")
    .alias("LAST_WORKING_DATE"),
    replace_empty_string_with_null("service.LEAVING_REASON_CODE").alias(
        "LEAVING_REASON_CODE"
    ),
    replace_empty_string_with_null("service.LEAVING_REASON").alias(
        "LEAVING_REASON_NAME"
    ),
    replace_empty_string_with_null("emp.ACCOMMODATION_TYPE").alias(
        "ACCOMMODATION_TYPE"
    ),
    replace_empty_string_with_null("emp.NATIONALITY_CATEGORY").alias(
        "NATIONALITY_CATEGORY"
    ),
    replace_empty_string_with_null("emp.POSITION_START_DATE")
    .cast("timestamp")
    .alias("POSITION_START_DATE"),
    replace_empty_string_with_null("emp.LEAVERS_CATEGORY").alias("LEAVERS_CATEGORY"),
    replace_empty_string_with_null("emp.EMP_LOCATION").alias("EMPLOYEE_LOCATION"),
    replace_empty_string_with_null("emp.EMP_STATUS").alias("EMPLOYEE_STATUS"),
    replace_empty_string_with_null("emp.POINT_OF_ORIGIN").alias("POINT_OF_ORIGIN"),
    replace_empty_string_with_null("emp.POINT_OF_ORIGIN_CODE").alias(
        "POINT_OF_ORIGIN_CODE"
    ),
    replace_empty_string_with_null("emp.EMP_CATEGORY").alias("EMPLOYEE_CATEGORY"),
    replace_empty_string_with_null("service.ACTUAL_TERMINATION_DATE")
    .cast("timestamp")
    .alias("ACTUAL_TERMINATION_DATE"),
    replace_empty_string_with_null("service.FINAL_PROCESS_DATE")
    .cast("timestamp")
    .alias("FINAL_PROCESS_DATE"),
    replace_empty_string_with_null("service.LAST_STANDARD_PROCESS_DATE")
    .cast("timestamp")
    .alias("LAST_STANDARD_PROCESS_DATE"),
    replace_empty_string_with_null("service.RESIGNATION_DATE")
    .cast("timestamp")
    .alias("RESIGNATION_DATE"),
    replace_empty_string_with_null("service.LAST_PAYMENT_DATE")
    .cast("timestamp")
    .alias("LAST_PAYMENT_DATE"),
    replace_empty_string_with_null("service.PROJECTED_LAST_DAY")
    .cast("timestamp")
    .alias("PROJECTED_LAST_DAY"),
    replace_empty_string_with_null("emp.COMPANY_CODE").alias("COMPANY_ID"),
)

df_final = df.distinct()

# COMMAND ----------

from pyspark.sql.functions import lit


df_final = apply_schema_casting(df_final,dim_employee_schema)

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["EMPLOYEE_NUMBER", "EMPLOYEE_ID"]

# Find duplicates
duplicate_df = (
    df_final
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in df_final based on keys {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in df_final.")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Schema Enforcement and writing final silver table to UC

# COMMAND ----------

# Build DDL string from schema dict
dim_employee_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_employee_schema.items()
)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_employee_schema_ddl)
    if compare_schema_lenient(df_final.schema, ddl_struct):
        print("Schema matches!")
        df_final.write.format("delta")\
                        .option("overwriteSchema", "true")\
                        .mode("overwrite")\
                        .saveAsTable(slv_table)
    else:
        print("Schema mismatch!")
        # displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        # displayHTML(f"<b>Actual:</b><br><pre>{df_final.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise Exception(f"FAIL: {str(e)}")  # Fails the notebook in workflow/task
