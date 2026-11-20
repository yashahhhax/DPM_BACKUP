# Databricks notebook source
# MAGIC %md
# MAGIC **SUMMARY**
# MAGIC
# MAGIC **Source table1**: bronze_omni.vw_dbo_vw_accounts
# MAGIC
# MAGIC **Source table2**: bronze_scv.vw_dbo_t_mdm_guest
# MAGIC
# MAGIC **Target silver table**: slv_shared.prep_dim_b2c_account
# MAGIC
# MAGIC Schema Enforcement implemented

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols": ["ACCOUNT_ID"]

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing required libraries**

# COMMAND ----------

# Import required functions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %md
# MAGIC **Environment parameters will be passed from workflow**

# COMMAND ----------

# dbutils.widgets.text("env", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Contains the expected schemas for Travel Box**

# COMMAND ----------

# MAGIC %run ../../config/schema_config

# COMMAND ----------

# MAGIC %md
# MAGIC **Contains all the base functions required**

# COMMAND ----------

# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Base function call to receive the param values**

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

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Define Source and Target Tables**

# COMMAND ----------

# env = dbutils.widgets.get("env")

# bronze table references
brz_vw_accounts = f"transformation_{env}.brz_omni.vw_dbo_vw_accounts"
brz_t_mdm_guest = f"transformation_{env}.brz_scv.vw_dbo_t_mdm_guest"

# silver schema and table
silver_schema = f"transformation_{env}.slv_shared"
silver_table = f"{silver_schema}.prep_dim_b2c_account"
# print(silver_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning of Bronze Source Tables

# COMMAND ----------

# Read dbo_vw_accounts
brz_vw_account_df = spark.read.table(brz_vw_accounts)

# Read dbo_t_mdm_guest  
brz_t_mdm_guest_df = spark.read.table(brz_t_mdm_guest)


# COMMAND ----------

# MAGIC %md
# MAGIC **Applying required changes on Schema**

# COMMAND ----------

brz_vw_account_df = brz_vw_account_df.select(
    "yasid",
    "accountid",
    "edw_accountid",
    "insdatetime",
    "upddatetime",
    "dmgcategoryid",
    "dmgcategorycode",
    "dmgcategorydesc",
    "recursivename",
    "dmgsubtype",
    "accountak",
    "firstname",
    "surname",
    "address1",
    "companyname",
    "zipcode",
    "city",
    "state",
    "country",
    "homephone",
    "businessphone",
    "fax",
    "mobilephone",
    "emailaddress1",
    "externalcode1",
    "additionalinfo1",
    "middlename",
    "title",
    "otherphone1",
    "otherphone2",
    "gender",
    "personalwebpage",
    "emailaddress2",
    "spousename",
    "birthdate",
    "county",
    "acknewsletter",
    "companyaddress1",
    "companyaddress2",
    "companycity",
    "passportphoto",
    "jobtitle",
    "province",
    "custom31_accountid",
    "custom32_accountak",
    "custom35",
    "webinfo01",
    "webinfo02",
    "etihadmemberid",
    "accounttype",
    "parentaccountid",
    "custom1"
).distinct()

# Normalize email
brz_t_mdm_guest_df = brz_t_mdm_guest_df.select(
    col("mdm_id"),
    col("yas_id"),
    lit(0).alias("source_guest_id"),
    col("industry"),
    col("emirate"),
    col("dob"),
    col("gender"),
    col("marital_status"),
    col("occupation"),
    col("state"),
    col("title"),
    col("eithiad_id"),
    col("guest_type"),
    col("status"),
    upper(col("email")).alias("email")
)

# COMMAND ----------

brz_vw_account_df.createOrReplaceTempView("Vw_Accounts")
brz_t_mdm_guest_df.createOrReplaceTempView("t_mdm_guest")

# COMMAND ----------

b2c_account_query = """ 
select 
CASE WHEN ACCOUNT_ID='' then NULL ELSE ACCOUNT_ID END AS ACCOUNT_ID,
CASE WHEN B2C_ACCOUNT_CODE='' then NULL ELSE B2C_ACCOUNT_CODE END AS B2C_ACCOUNT_CODE,
CASE WHEN TITLE='' then NULL ELSE TITLE END AS TITLE,
CASE WHEN FIRST_NAME='' then NULL ELSE FIRST_NAME END AS FIRST_NAME,
CASE WHEN MIDDLE_NAME='' then NULL ELSE MIDDLE_NAME END AS MIDDLE_NAME,
CASE WHEN LAST_NAME='' then NULL ELSE LAST_NAME END AS LAST_NAME,
CASE WHEN CUSTOMER_TYPE='' then NULL ELSE CUSTOMER_TYPE END AS CUSTOMER_TYPE,
CASE WHEN COUNTRY_RESIDENCE='' then NULL ELSE COUNTRY_RESIDENCE END AS COUNTRY_RESIDENCE,
CASE WHEN COUNTRY_NATIONALITY='' then NULL ELSE COUNTRY_NATIONALITY END AS COUNTRY_NATIONALITY,
CASE WHEN ADDRESS='' then NULL ELSE ADDRESS END AS ADDRESS,
CASE WHEN CITY='' then NULL ELSE CITY END AS CITY,
CASE WHEN PO_BOX_NUMBER='' then NULL ELSE PO_BOX_NUMBER END AS PO_BOX_NUMBER,
CASE WHEN EMIRATE_STATE='' then NULL ELSE EMIRATE_STATE END AS EMIRATE_STATE,
CASE WHEN ZIP_PINCODE='' then NULL ELSE ZIP_PINCODE END AS ZIP_PINCODE,
CASE WHEN DOB='' then NULL ELSE DOB END AS DOB,
CASE WHEN EMAIL_ADDRESS='' then NULL ELSE EMAIL_ADDRESS END AS EMAIL_ADDRESS,
CASE WHEN MOBILE_NUMBER='' then NULL ELSE MOBILE_NUMBER END AS MOBILE_NUMBER,
CASE WHEN FAX_NUMBER='' then NULL ELSE FAX_NUMBER END AS FAX_NUMBER,
CASE WHEN GENDER='' then NULL ELSE GENDER END AS GENDER,
CASE WHEN ETIHAD_ID='' then NULL ELSE ETIHAD_ID END AS ETIHAD_ID,
CASE WHEN GUEST_TYPE='' then NULL ELSE GUEST_TYPE END AS GUEST_TYPE,
CASE WHEN INDUSTRY='' then NULL ELSE INDUSTRY END AS INDUSTRY,
CASE WHEN LANGUAGE='' then NULL ELSE LANGUAGE END AS LANGUAGE,
CASE WHEN MARITAL_STATUS='' then NULL ELSE MARITAL_STATUS END AS MARITAL_STATUS,
CASE WHEN OCCUPATION='' then NULL ELSE OCCUPATION END AS OCCUPATION,
CASE WHEN MDM_ID='' then NULL ELSE MDM_ID END AS MDM_ID,
CASE WHEN SCV_ID='' then NULL ELSE SCV_ID END AS SCV_ID,
CASE WHEN YAS_ID='' then NULL ELSE YAS_ID END AS YAS_ID,
CASE WHEN STATUS='' then NULL ELSE STATUS END AS STATUS,
CASE WHEN SOURCE_SYSTEM_NAME='' then NULL ELSE SOURCE_SYSTEM_NAME END AS SOURCE_SYSTEM_NAME,
CASE WHEN EMAIL='' then NULL ELSE EMAIL END AS EMAIL
from(
  select distinct 
    data_account.AccountAk as B2C_ACCOUNT_CODE,
    scv1.title as TITLE, 
    initcap(data_account.FirstName) as FIRST_NAME, 
    '' as MIDDLE_NAME, 
    initcap(data_account.SurName) as LAST_NAME, 
    '' as CUSTOMER_TYPE, 
    -- data_account.County as COUNTRY_RESIDENCE,
    -- data_account.Country as COUNTRY_NATIONALITY,
    CASE WHEN data_account.Country RLIKE '^[./*㜋࣯()0-9-]+$' or len(data_account.Country)>2 THEN 'UNKNOWN' ELSE data_account.Country END as COUNTRY_RESIDENCE,
    CASE WHEN data_account.Custom1 RLIKE '^[./*()0-9-]+$' or len(data_account.Custom1)>2 THEN 'UNKNOWN' ELSE  data_account.Custom1  END as COUNTRY_NATIONALITY,
    data_account.Address1 as ADDRESS, 
    data_account.City as CITY,
    '' as PO_BOX_NUMBER,
    scv1.emirate as EMIRATE_STATE,
    data_account.ZipCode as ZIP_PINCODE, 
    cast(scv1.dob as Timestamp) as DOB, 
    lower(data_account.EmailAddress1) as EMAIL_ADDRESS,
    --regexp_replace(lower(data_account.EmailAddress1), '^([a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}).*', '\\\\1') AS EMAIL_ADDRESS,
    data_account.MobilePhone as MOBILE_NUMBER,
    data_account.Fax as FAX_NUMBER, 
    scv1.gender as GENDER, 
    scv1.eithiad_id as ETIHAD_ID,
    scv1.guest_type as GUEST_TYPE, 
    scv1.industry as INDUSTRY,
    '' as LANGUAGE,
    scv1.marital_status as MARITAL_STATUS, 
    scv1.occupation as OCCUPATION, 
    scv1.mdm_id as MDM_ID, 
    data_account.YASID as SCV_ID, 
    scv1.yas_id as YAS_ID,
    scv1.status as STATUS,
    cast(data_account.AccountId as Decimal(38,0)) as ACCOUNT_ID,
    'OMNI' as SOURCE_SYSTEM_NAME,
    scv1.email as EMAIL
  from Vw_Accounts data_account 
  left join (
    select *, row_number() over (partition by email order by mdm_id desc) as rn  
    from t_mdm_guest
  ) scv1 
  on lower(scv1.email)=lower(data_account.EmailAddress1) 
  and scv1.rn=1 
) 
"""

# COMMAND ----------

b2c_account_df = spark.sql(b2c_account_query).select(
    "ACCOUNT_ID",
    "B2C_ACCOUNT_CODE",
    "TITLE",
    "FIRST_NAME",
    "MIDDLE_NAME",
    "LAST_NAME",
    "CUSTOMER_TYPE",
    "COUNTRY_RESIDENCE",
    "COUNTRY_NATIONALITY",
    "ADDRESS",
    "CITY",
    "PO_BOX_NUMBER",
    "EMIRATE_STATE",
    "ZIP_PINCODE",
    "DOB",
    "EMAIL_ADDRESS",
    "MOBILE_NUMBER",
    "FAX_NUMBER",
    "GENDER",
    "ETIHAD_ID",
    "GUEST_TYPE",
    "INDUSTRY",
    "LANGUAGE",
    "MARITAL_STATUS",
    "OCCUPATION",
    "MDM_ID",
    "SCV_ID",
    "YAS_ID",
    "STATUS",
    "SOURCE_SYSTEM_NAME",
    "EMAIL"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Schema Casting

# COMMAND ----------

from pyspark.sql.functions import lit

# Standardize column names
def clean_col(col_name):
    cleaned = re.sub(r"[.\s]+", "_", col_name)         # Replace dots/spaces with _
    cleaned = re.sub(r"[^\w]", "_", cleaned)          # Replace other special chars
    cleaned = re.sub(r"__+", "_", cleaned)            # Collapse multiple underscores
    cleaned = cleaned.strip("_")                      # Trim leading/trailing _
    return cleaned.upper()

new_col_names = [clean_col(c) for c in b2c_account_df.columns]
b2c_account_df = b2c_account_df.toDF(*new_col_names)

b2c_account_df = apply_schema_casting(b2c_account_df, dim_b2c_account_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Enforcement and then writing final silver table to UC**

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
primary_keys = ["ACCOUNT_ID"]

# Find duplicates
duplicate_df = (
    b2c_account_df
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional: display duplicates
    raise Exception(f"❌ Duplicates found in b2c_account_df based on key {primary_keys}. Failing notebook.")
else:
    print("✅ No duplicates found in b2c_account_df.")


# COMMAND ----------

# Build DDL string from schema dict
dim_b2c_account_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_b2c_account_schema.items()
)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_b2c_account_schema_ddl)
    if compare_schema_lenient(b2c_account_df.schema, ddl_struct):
        print("Schema matches!")
        b2c_account_df.write.format("delta")\
                        .option("overwriteSchema", "true")\
                        .mode("overwrite")\
                        .saveAsTable(silver_table)
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{b2c_account_df.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise Exception(f"FAIL: {str(e)}")  # Fails the notebook in workflow/task
