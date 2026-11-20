# Databricks notebook source
#Target silver table: prep_dim_master_hotel_names


# COMMAND ----------

# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,Run base_functions
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# %run /Workspace/Shared/MX-DATA-ADB-TRAVELBOX/shared_code/common_functions/base_functions

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

# Import necessary libraries
import json
from pyspark.sql.functions import col, when, current_timestamp
from pyspark.sql.types import StructType

# COMMAND ----------

# Define Source and Target Tables
# bronze_schema = f"transformation_{env}.brz_travelbox"
# silver_schema = f"transformation_{env}.slv_travelbox"
silver_schema = f"transformation_{env}.slv_travelbox"
bronze_schema = f"transformation_{env}.brz_sharepoint_list"
bronze_master_hotel_list = f"{bronze_schema}.vw_farahflfin_hotelsmasterlist"
silver_table = f"{silver_schema}.prep_dim_master_hotel_name"

# COMMAND ----------

print(silver_table)

# COMMAND ----------

# Performing few cleaning steps like de-duplicacy and null rows removals
clean_master_table_name_df = clean_and_return_df(bronze_master_hotel_list)

# COMMAND ----------

# DBTITLE 1,Column mapping and schema casting
# Define your required columns and their target names
column_mapping = {
    "TITLE": "BUSINESS_ALIAS",
    "ACTIVEINACTIVE":"STATUS",
    "SOURCEHOTELNAME":"SOURCE_HOTEL_NAME",
    "GROUPNAMEYASPLAZAHILTONCLUSTER":"GROUP_NAME",
    "B2BB2CB2BONLY":"SELLING_CHANNEL",
    "YNHB2BB2CORB2BONLY":"YNH_SELLING_CHANNEL",
    "YASISLANDSAADIYATISLANDABUDHABICITYALAINLIWAMIRFA":"LOCATION",
    "T1T1YNHPT2DAILYPARKACCESSSPECIALMODEL":"PRICING_TIER_TICKETING_RESELLERS",
    "TBXSUPPLIERCODE" :"HOTEL_CODE"
}

# Apply select and rename in one shot
master_hotel_name_df = clean_master_table_name_df.select([
    col(original).alias(renamed) for original, renamed in column_mapping.items()
])  

# Apply schema casting
master_hotel_name_df = apply_schema_casting(master_hotel_name_df, dim_master_hotel_name_schema)

# Remove duplicates and nulls based on all selected columns
master_hotel_name_df = master_hotel_name_df.dropna(subset=["source_hotel_name", "BUSINESS_ALIAS"])
# master_hotel_name_df = master_hotel_name_df.dropDuplicates(subset=["source_hotel_name"])
# Case-insensitive deduplication to avoid duplicates when view joins use lower()
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lower, trim
window_spec = Window.partitionBy(lower(trim(col("source_hotel_name")))).orderBy(col("source_hotel_name"))
master_hotel_name_df = master_hotel_name_df.withColumn("_rn", row_number().over(window_spec)) \
    .filter(col("_rn") == 1) \
    .drop("_rn")
#------------------------------------End------------------------------------------------

# COMMAND ----------

master_hotel_name_df.display()

# COMMAND ----------

# DBTITLE 1,Schema Enforcement and then writing final silver table to UC
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_master_hotel_name_schema_ddl)
    if compare_schema_lenient(master_hotel_name_df.schema, ddl_struct):
        print("Schema matches!")
        # displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        # displayHTML(f"<b>Actual:</b><br><pre>{supplier_df.schema}</pre>")
        master_hotel_name_df.display()
        master_hotel_name_df.write.format("delta")\
                            .option("mergeSchema", "true")\
                            .mode("overwrite")\
                            .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        # displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        # displayHTML(f"<b>Actual:</b><br><pre>{supplier_df.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise Exception(f"FAIL: {str(e)}")

