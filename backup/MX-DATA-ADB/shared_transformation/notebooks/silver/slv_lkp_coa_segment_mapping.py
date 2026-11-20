# Databricks notebook source
# MAGIC %md
# MAGIC ## Pipeline Initialization & Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC Source : transformation_{env}.brz_sharepoint_list.vw_farahflfin_coa_segment_mapping
# MAGIC
# MAGIC Target_Slv : transformation_{env}.slv_drr.prep_lkp_coa_segment_mapping
# MAGIC
# MAGIC Target_Gld : transformation_{env}.gld_drr.dwh_lkp_coa_segment_mapping

# COMMAND ----------

# MAGIC %md
# MAGIC ### 
# MAGIC Enviornment parameter will be passed from the workflow

# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.functions import when, col, lit, current_timestamp
from pyspark.sql.functions import when, col
from pyspark.sql.types import IntegerType


# COMMAND ----------

dbutils.widgets.text("env","")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Contains expected schemas for all the tables

# COMMAND ----------

# MAGIC %run ../../config/schema_config

# COMMAND ----------

# MAGIC %md
# MAGIC #### Contains all the base functions required 

# COMMAND ----------

# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Base function call to receive the param values 

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
# MAGIC #### Define Source and Target Tables based on the environment variable 

# COMMAND ----------

bronze_coa_segment_mapping = f"transformation_{env}.brz_sharepoint_list.vw_farahflfin_coa_segment_mapping"
silver_schema = f"transformation_{env}.slv_shared"
gold_schema = f"transformation_{env}.gld_shared"
silver_coa_segment_mapping= f"{silver_schema}.prep_lkp_coa_segment_mapping"
gold_coa_segment_mapping = f"{gold_schema}.dwh_lkp_coa_segment_mapping"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read the bronze table data 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Clean the data from the bronze table and return it as a DataFrame 

# COMMAND ----------

cleaned_coa_segment_mapping_df = clean_and_return_df(bronze_coa_segment_mapping)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Selecting required columns from the cleaned DataFrame and schema casting 

# COMMAND ----------

required_columns = [
'NEW_CODE',
'NEW_DESCRIPTION',
'OLD_CODE',
'OLD_DESCRIPTION',
'SEGMENT_NAME'
]


# COMMAND ----------

coa_segment_mapping_df = cleaned_coa_segment_mapping_df.select(*required_columns)

# COMMAND ----------

# DBTITLE 1,transformations


transformed_df = coa_segment_mapping_df.select(
    when(col("NEW_CODE") == "", None).otherwise(col("NEW_CODE")).alias("NEW_CODE"),
    when(col("NEW_DESCRIPTION") == "", None).otherwise(col("NEW_DESCRIPTION")).alias("NEW_DESCRIPTION"),
    when(col("OLD_CODE") == "", None).otherwise(col("OLD_CODE")).alias("OLD_CODE"),
    when(col("OLD_DESCRIPTION") == "", None).otherwise(col("OLD_DESCRIPTION")).alias("OLD_DESCRIPTION"),
    when(col("SEGMENT_NAME") == "", None).otherwise(col("SEGMENT_NAME")).alias("SEGMENT_NAME")
)


# COMMAND ----------

from pyspark.sql.functions import col, when

transformed_df = transformed_df.select([
    when(col(c) == '', None).otherwise(col(c)).alias(c)
    for c in transformed_df.columns
])


#Apply casting
transformed_df = apply_schema_casting(transformed_df, dim_coa_segment_mapping_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Schema Enforcement and then writing final silver table to [UC](url) 

# COMMAND ----------

# Build DDL string from schema dict
dim_coa_segment_mapping_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_coa_segment_mapping_schema.items()
)

# print(dim_coa_segment_mapping_schema_ddl)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_coa_segment_mapping_schema_ddl)
    if compare_schema_lenient(transformed_df.schema, ddl_struct):
        print("Schema matches!")
        transformed_df.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .saveAsTable(silver_coa_segment_mapping)
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{transformed_df.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e
