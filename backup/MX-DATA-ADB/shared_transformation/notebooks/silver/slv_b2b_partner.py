# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC ### Notebook Summary: Silver Layer Table – `b2b_partner`
# MAGIC
# MAGIC This notebook builds the Silver table `slv_shared.b2b_partner` using data from CRM, OMNI, and TravelBox systems. Below is a summary of the key activities:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. Import Required Libraries
# MAGIC - Loads essential PySpark functions and types.
# MAGIC
# MAGIC ### 2. Read Workflow Parameters
# MAGIC - Uses widgets to get `env` and `table_class` dynamically.
# MAGIC
# MAGIC ### 3. Load Configuration and Utility Functions
# MAGIC - Executes:
# MAGIC   - `schema_config` (for expected schema)
# MAGIC   - `base_functions` (for reusable utilities)
# MAGIC
# MAGIC ### 4. Define Source and Target Paths
# MAGIC - Constructs fully qualified table names for Bronze and Silver layers based on `env`.
# MAGIC
# MAGIC ### 5. Read and Clean Source Tables
# MAGIC - Reads and cleans all 8 Bronze source tables:
# MAGIC   - `vw_crm_dot_country`, `vw_crm_dot_region`, `vw_crm_dot_segment`
# MAGIC   - `vw_crm_account`, `vw_crm_fe_omnidmgcategory`
# MAGIC   - `vw_dbo_data_account`, `vw_dbo_data_dmgdata`, `vw_dbo_cnf_dmgcategory`
# MAGIC
# MAGIC ### 6. Build Intermediate Views
# MAGIC - `account_dmg_map`:  
# MAGIC   Joins `data_account`, `data_dmgdata`, and `cnf_dmgcategory`
# MAGIC - `account_country_map`:  
# MAGIC   Joins  `account_segment_map`:  
# MAGIC   Joins CRM account data with segment & region details
# MAGIC
# MAGIC ### 7. Combine and Transform Final Data
# MAGIC - Combines all enriched mappings
# MAGIC - Adds CRM, OMNI, and TravelBox columns
# MAGIC - Applies fallback logic and handles missing values
# MAGIC
# MAGIC ### 8. Deduplication
# MAGIC - Ensures uniqueness on `partner_crm_account_id`
# MAGIC
# MAGIC ### 9. Apply Schema Casting
# MAGIC - Merges schema definition and casts columns to expected types
# MAGIC
# MAGIC ### 10. Validate Schema and Write to Silver Table
# MAGIC - Compares with expected schema DDL  
# MAGIC - If matched, writes data to `slv_shared.prep_dim_b2b_partner` using Delta
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC "primary_key_cols": ["PARTNER_CRM_ACCOUNT_ID"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import necessary libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, NumericType
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enviornment parameter will be passed from the workflow

# COMMAND ----------

# dbutils.widgets.text("env","")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contains the expected schemas for DRR Objects used in Travel Box

# COMMAND ----------

# MAGIC %run ../../config/schema_config

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

# print(env)

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Source and Target Tables

# COMMAND ----------

# Define the brz table names
brz_dbo_data_account          = f"transformation_{env}.brz_omni.vw_dbo_data_account"
brz_dbo_data_dmgdata          = f"transformation_{env}.brz_omni.vw_dbo_data_dmgdata"
brz_crm_accounts              = f"transformation_{env}.brz_crm.vw_crm_account"
brz_crm_dot_regions           = f"transformation_{env}.brz_crm.vw_crm_dot_region"
brz_crm_dot_countries         = f"transformation_{env}.brz_crm.vw_crm_dot_country"
brz_crm_fe_omnidmgcategories  = f"transformation_{env}.brz_crm.vw_crm_fe_omnidmgcategory"
brz_crm_dot_segment           = f"transformation_{env}.brz_crm.vw_crm_dot_segment"
brz_dbo_cnf_dmgcategory       = f"transformation_{env}.brz_omni.vw_dbo_cnf_dmgcategory"

# Define the silver schema and table names
slv_schema                       = f"transformation_{env}.slv_shared"

# silver table name
slv_table                       = f"transformation_{env}.slv_shared.prep_dim_b2b_partner"

# COMMAND ----------

# DBTITLE 1,tw
spark.table(brz_dbo_data_account).createOrReplaceTempView("data_account")
spark.table(brz_dbo_data_dmgdata).createOrReplaceTempView("data_dmgdata")
spark.table(brz_crm_accounts).createOrReplaceTempView("accounts")
spark.table(brz_crm_dot_regions).createOrReplaceTempView("dot_regions")
spark.table(brz_crm_dot_countries).createOrReplaceTempView("dot_countries")
spark.table(brz_crm_fe_omnidmgcategories).createOrReplaceTempView("fe_omnidmgcategories")
spark.table(brz_crm_dot_segment).createOrReplaceTempView("dot_segment")
spark.table(brz_dbo_cnf_dmgcategory).createOrReplaceTempView("cnf_dmgcategory")

# COMMAND ----------

# DBTITLE 1,trtt
data_account_df = spark.sql("""
SELECT distinct data_account.AccountId,
		data_account.AccountAK,
		data_account.AccountType,
		data_account.Enabled,
		data_account.ParentAccountId,
		data_account.DmgCategoryId,
		data_account.DisplayName,
		data_account.FirstName,
		data_account.SurName,
		data_account.CompanyName,
		data_account.Address1,
		data_account.ZipCode,
		data_account.City,
		data_account.State,
		data_account.Country,
		data_dmgdata25.MaskData as County,
		data_account.HomePhone,
		data_account.BusinessPhone,
		data_account.Fax,
		data_account.MobilePhone,
		data_account.EmailAddress1,
		data_account.InsDateTime,
		data_account.UpdDateTime,
		data_account.ExternalCode1,
		data_account.UpdUserId,
		data_account.InsUserId,
		data_account.OfflineAccountAK,
		data_account.UserId,
		data_account.BillingAccountId,
		data_account.DisplayField2,
		data_account.DisplayField1,
		data_account.Password,
		data_account.AccountGUID,
		data_account.Membership,
		data_account.LoginAttempts,
		data_account.LanguageId,
		data_account.ExpirationDate
FROM data_account 
LEFT JOIN data_dmgdata as data_dmgdata25 ON data_dmgdata25.DmgType=1 AND data_dmgdata25.Fkey = data_account.Accountid AND data_dmgdata25.ObjType= 25
""")

# COMMAND ----------

data_account_df.createOrReplaceTempView("data_account")

# COMMAND ----------

get_account_dmg_map_df = spark.sql("""
SELECT 'FPK' AS YASID
,data_account.AccountId AS AccountId_1
	,data_account.InsDateTime AS InsDateTime_1
	,data_account.UpdDateTime AS UpdDateTime_1
	,cnf_dmgcat.DmgCategoryCode AS DmgCategoryCode
	,cnf_dmgcat.Description AS Description
	,cnf_dmgcat.RecursiveName AS RecursiveName
	,cnf_dmgcat.DmgParentId AS DmgParentId
	-- ,cnf_dmgcat.OMNI_ACCOUNT_PARENT_CAT_DMG_CODE AS OMNI_ACCOUNT_PARENT_CAT_DMG_CODE
	-- ,cnf_dmgcat.OMNI_ACCOUNT_PARENT_CAT_DMG_DESCRIPTION AS OMNI_ACCOUNT_PARENT_CAT_DMG_DESCRIPTION
	-- ,cnf_dmgcat.OMNI_ACCOUNT_PARENT_CAT_DMG_RECURSIVE_NAME AS OMNI_ACCOUNT_PARENT_CAT_DMG_RECURSIVE_NAME
	,cnf_dmgcat.DmgSubType AS DmgSubType
	,data_account.AccountAK AS AccountAk
	,data_account.FirstName AS FirstName_1
	,data_account.SurName AS SurName_1
	,data_account.Address1 AS Address1_1
	,data_account.CompanyName AS CompanyName_1
	,data_account.ZipCode AS ZipCode_1
	,data_account.City AS City_1
	,data_account.STATE AS State_1
	,data_account.Country AS Country_1
	,data_account.HomePhone AS HomePhone_1
	,data_account.BusinessPhone AS BusinessPhone_1
	,data_account.Fax AS Fax_1
	,data_account.MobilePhone AS MobilePhone_1
	,data_account.EmailAddress1 AS EmailAddress1_1
	,data_account.ExternalCode1 AS ExternalCode1_1
	,data_account.DisplayField1 AS AdditionalInfo1
	,data_account.SurName AS MiddleName
	,data_account.AccountType AS AccountType
FROM data_account AS data_account
LEFT JOIN cnf_dmgcategory as cnf_dmgcat ON data_account.DmgCategoryId = cnf_dmgcat.DmgCategoryId
""")

# COMMAND ----------

get_account_dmg_map_df.createOrReplaceTempView("account_dmg_map")

# COMMAND ----------

get_account_country_map = spark.sql(""" 
select Accounts.accountid as accountid,
  Accounts.accountnumber as accountnumber,
  Accounts.name as name,
  Accounts.parentaccountid as parentaccountid,
  Accounts.dot_qualifiedaccounttravelbox as dot_qualifiedaccounttravelbox,
  Accounts.telephone1 as telephone1,
  Accounts.fe_omnidmgcategory as fe_omnidmgcategory,
  Accounts.yc_omniaccountid as yc_omniaccountid,
  Accounts.dot_subsegmentid as dot_subsegmentid,
  Accounts.emailaddress1 as emailaddress1,
  Accounts.dot_segmentid as dot_segmentid,
  "" as dot_segment,
  "" as dot_subsegment ,
  case when Accounts.yc_isehdmc="TRUE" then "Y" else "N"  end as yc_isehdmc , 
  Accounts.parentaccountidname as _parentaccountid_value_lkp , 
  case when Accounts.statecode = 0 then "Active" else "Inactive"  end as ACTIVE_FLAG, 
  case when ISNULL (Accounts.statuscode) then "" 
  when Accounts.statuscode = 2 then "Inactive"
  when Accounts.statuscode = 1 then "Contracted" else "Non Contracted" end  as CONTRACT_STATUS, 
  case when Accounts.modifiedon = "" then "" else unix_timestamp(LEFT(cast(from_utc_timestamp(Accounts.modifiedon , "Asia/Dubai")as string),19)) end as  CRM_LAST_MODIFIED_DATE,
  Accounts.dot_country  as COUNTRY_VALUE , 
  Countries1.dot_name  as COUNTRY_NAME , 
  Regions1.dot_name  as REGION_NAME ,
  Regions1.dot_regionid as REGION_CODE,
  Countries1.dot_countryid as COUNTRY_CODE
  from accounts Accounts 
  left outer join dot_regions Regions1 on Accounts.dot_region = Regions1.dot_regionid 
  left outer join dot_countries Countries1 on Accounts.dot_country=Countries1.dot_countryid
  """)

# COMMAND ----------

get_account_country_map.createOrReplaceTempView("account_country_map")

# COMMAND ----------

get_account_segment_map_df = spark.sql("""
select out1.accountid as accountid,
  out1.accountnumber as accountnumber,
  out1.name as name,
  out1.parentaccountid as parentaccountid,
  out1.telephone1 as telephone1,
  out1.fe_omnidmgcategory as fe_omnidmgcategory,
  out1.yc_omniaccountid as yc_omniaccountid,
  out1.dot_subsegmentid as dot_subsegmentid,
  out1.emailaddress1 as emailaddress1,
  out1.dot_segmentid as dot_segmentid,
  out1.dot_segment as dot_segment,
  out1.dot_subsegment as dot_subsegment,
  out1.yc_isehdmc as yc_isehdmc,
  out1._parentaccountid_value_lkp as _parentaccountid_value_lkp,
  sub_segment.dot_segmentid  as dot_segmentid_lkp,
  sub_segment.dot_name  as dot_sub_seg_name_lkp,
  dot_segment.dot_name  as dot_seg_name,
  out1.ACTIVE_FLAG  as ACTIVE_FLAG,
  out1.CONTRACT_STATUS  as CONTRACT_STATUS,
  out1.CRM_LAST_MODIFIED_DATE  as CRM_LAST_MODIFIED_DATE,
  out1.COUNTRY_VALUE as COUNTRY_CODE,
  out1.COUNTRY_NAME as COUNTRY_NAME,
  out1.REGION_CODE AS REGION_CODE,
  out1.REGION_NAME as REGION_NAME 
  from account_country_map out1 
  left outer join dot_segment sub_segment on out1.dot_subsegmentid= sub_segment.dot_segmentid 
  left outer join dot_segment on out1.dot_segmentid=dot_segment.dot_segmentid
  """)

# COMMAND ----------

get_account_segment_map_df.createOrReplaceTempView("account_segment_map")

# COMMAND ----------

b2b_partner_df = spark.sql("""
select CASE WHEN ACCOUNT_CATEGORY='' then NULL ELSE ACCOUNT_CATEGORY END AS ACCOUNT_CATEGORY,
CASE WHEN ACCOUNT_ID='' then NULL ELSE ACCOUNT_ID END AS ACCOUNT_ID,
CASE WHEN ACTIVE_FLAG='' then NULL ELSE ACTIVE_FLAG END AS ACTIVE_FLAG,
CASE WHEN CONTRACT_STATUS='' then NULL ELSE CONTRACT_STATUS END AS CONTRACT_STATUS,
CASE WHEN COUNTRY_CODE='' then NULL ELSE COUNTRY_CODE END AS COUNTRY_CODE,
CASE WHEN COUNTRY_NAME='' then NULL ELSE COUNTRY_NAME END AS COUNTRY_NAME,
CASE WHEN CRM_LAST_MODIFIED_DATE='' then NULL ELSE CRM_LAST_MODIFIED_DATE END AS CRM_LAST_MODIFIED_DATE,
CASE WHEN EH_DMC_FLAG='' then NULL ELSE EH_DMC_FLAG END AS EH_DMC_FLAG,
CASE WHEN KEY_ACCOUNT_FLAG='' then NULL ELSE KEY_ACCOUNT_FLAG END AS KEY_ACCOUNT_FLAG,
CASE WHEN OMNI_ACCOUNT_CAT_DMG_CODE='' then NULL ELSE OMNI_ACCOUNT_CAT_DMG_CODE END AS OMNI_ACCOUNT_CAT_DMG_CODE,
CASE WHEN OMNI_ACCOUNT_CAT_DMG_DESCRIPTION='' then NULL ELSE OMNI_ACCOUNT_CAT_DMG_DESCRIPTION END AS OMNI_ACCOUNT_CAT_DMG_DESCRIPTION,
CASE WHEN OMNI_ACCOUNT_CAT_DMG_PARENT='' then NULL ELSE OMNI_ACCOUNT_CAT_DMG_PARENT END AS OMNI_ACCOUNT_CAT_DMG_PARENT,
CASE WHEN OMNI_ACCOUNT_CAT_DMG_RECURSIVE_NAME='' then NULL ELSE OMNI_ACCOUNT_CAT_DMG_RECURSIVE_NAME END AS OMNI_ACCOUNT_CAT_DMG_RECURSIVE_NAME,
CASE WHEN OMNI_ACCOUNT_PARENT_CAT_DMG_CODE='' then NULL ELSE OMNI_ACCOUNT_PARENT_CAT_DMG_CODE END AS OMNI_ACCOUNT_PARENT_CAT_DMG_CODE,
CASE WHEN OMNI_ACCOUNT_PARENT_CAT_DMG_DESCRIPTION='' then NULL ELSE OMNI_ACCOUNT_PARENT_CAT_DMG_DESCRIPTION END AS OMNI_ACCOUNT_PARENT_CAT_DMG_DESCRIPTION,
CASE WHEN OMNI_ACCOUNT_PARENT_CAT_DMG_RECURSIVE_NAME='' then NULL ELSE OMNI_ACCOUNT_PARENT_CAT_DMG_RECURSIVE_NAME END AS OMNI_ACCOUNT_PARENT_CAT_DMG_RECURSIVE_NAME,
CASE WHEN PARENT_PARTNER_CRM_ACCOUNT_ID='' then NULL ELSE PARENT_PARTNER_CRM_ACCOUNT_ID END AS PARENT_PARTNER_CRM_ACCOUNT_ID,
CASE WHEN PARENT_PARTNER_CRM_ACCOUNT_NAME='' then NULL ELSE PARENT_PARTNER_CRM_ACCOUNT_NAME END AS PARENT_PARTNER_CRM_ACCOUNT_NAME,
CASE WHEN PARTNER_CRM_ACCOUNT_EMAIL='' then NULL ELSE PARTNER_CRM_ACCOUNT_EMAIL END AS PARTNER_CRM_ACCOUNT_EMAIL,
CASE WHEN PARTNER_CRM_ACCOUNT_ID='' then NULL ELSE PARTNER_CRM_ACCOUNT_ID END AS PARTNER_CRM_ACCOUNT_ID,
CASE WHEN PARTNER_CRM_ACCOUNT_NAME='' then NULL ELSE PARTNER_CRM_ACCOUNT_NAME END AS PARTNER_CRM_ACCOUNT_NAME,
CASE WHEN PARTNER_CRM_ACCOUNT_Number='' then NULL ELSE PARTNER_CRM_ACCOUNT_Number END AS PARTNER_CRM_ACCOUNT_Number,
CASE WHEN PARTNER_CRM_ACCOUNT_PHONE_NUMBER='' then NULL ELSE PARTNER_CRM_ACCOUNT_PHONE_NUMBER END AS PARTNER_CRM_ACCOUNT_PHONE_NUMBER,
CASE WHEN PARTNER_CRM_SEG_CODE='' then NULL ELSE PARTNER_CRM_SEG_CODE END AS PARTNER_CRM_SEG_CODE,
CASE WHEN PARTNER_CRM_SEG_NAME='' then NULL ELSE PARTNER_CRM_SEG_NAME END AS PARTNER_CRM_SEG_NAME,
CASE WHEN PARTNER_CRM_SUB_SEG_CODE='' then NULL ELSE PARTNER_CRM_SUB_SEG_CODE END AS PARTNER_CRM_SUB_SEG_CODE,
CASE WHEN PARTNER_CRM_SUB_SEG_NAME='' then NULL ELSE PARTNER_CRM_SUB_SEG_NAME END AS PARTNER_CRM_SUB_SEG_NAME,
CASE WHEN PARTNER_OMNI_ACCOUNT_CAT_DMG_CODE='' then NULL ELSE PARTNER_OMNI_ACCOUNT_CAT_DMG_CODE END AS PARTNER_OMNI_ACCOUNT_CAT_DMG_CODE,
CASE WHEN PARTNER_OMNI_ACCOUNT_CAT_DMG_NAME='' then NULL ELSE PARTNER_OMNI_ACCOUNT_CAT_DMG_NAME END AS PARTNER_OMNI_ACCOUNT_CAT_DMG_NAME,
CASE WHEN PARTNER_OMNI_ACCOUNT_EMAIL='' then NULL ELSE PARTNER_OMNI_ACCOUNT_EMAIL END AS PARTNER_OMNI_ACCOUNT_EMAIL,
CASE WHEN PARTNER_OMNI_ACCOUNT_ID='' then NULL ELSE PARTNER_OMNI_ACCOUNT_ID END AS PARTNER_OMNI_ACCOUNT_ID,
CASE WHEN PARTNER_OMNI_ACCOUNT_NAME='' then NULL ELSE PARTNER_OMNI_ACCOUNT_NAME END AS PARTNER_OMNI_ACCOUNT_NAME,
CASE WHEN PARTNER_OMNI_ACCOUNT_PHONE_NUMBER='' then NULL ELSE PARTNER_OMNI_ACCOUNT_PHONE_NUMBER END AS PARTNER_OMNI_ACCOUNT_PHONE_NUMBER,
CASE WHEN PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_CODE='' then NULL ELSE PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_CODE END AS PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_CODE,
CASE WHEN PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_NAME='' then NULL ELSE PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_NAME END AS PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_NAME,
CASE WHEN PARTNER_OMNI_YAS_ARENA_ACCOUNT_EMAIL_ADDRESS='' then NULL ELSE PARTNER_OMNI_YAS_ARENA_ACCOUNT_EMAIL_ADDRESS END AS PARTNER_OMNI_YAS_ARENA_ACCOUNT_EMAIL_ADDRESS,
CASE WHEN PARTNER_OMNI_YAS_ARENA_ACCOUNT_ID='' then NULL ELSE PARTNER_OMNI_YAS_ARENA_ACCOUNT_ID END AS PARTNER_OMNI_YAS_ARENA_ACCOUNT_ID,
CASE WHEN PARTNER_OMNI_YAS_ARENA_ACCOUNT_NAME='' then NULL ELSE PARTNER_OMNI_YAS_ARENA_ACCOUNT_NAME END AS PARTNER_OMNI_YAS_ARENA_ACCOUNT_NAME,
CASE WHEN PARTNER_OMNI_YAS_ARENA_ACCOUNT_PHONE_NUMBER='' then NULL ELSE PARTNER_OMNI_YAS_ARENA_ACCOUNT_PHONE_NUMBER END AS PARTNER_OMNI_YAS_ARENA_ACCOUNT_PHONE_NUMBER,
CASE WHEN PARTNER_ORACLE_ACCOUNT_ID='' then NULL ELSE PARTNER_ORACLE_ACCOUNT_ID END AS PARTNER_ORACLE_ACCOUNT_ID,
CASE WHEN PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_CODE='' then NULL ELSE PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_CODE END AS PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_CODE,
CASE WHEN PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_NAME='' then NULL ELSE PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_NAME END AS PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_NAME,
CASE WHEN PARTNER_TRAVEL_BOX_ACCOUNT_EMAIL_ADDRESS='' then NULL ELSE PARTNER_TRAVEL_BOX_ACCOUNT_EMAIL_ADDRESS END AS PARTNER_TRAVEL_BOX_ACCOUNT_EMAIL_ADDRESS,
CASE WHEN PARTNER_TRAVEL_BOX_ACCOUNT_ID='' then NULL ELSE PARTNER_TRAVEL_BOX_ACCOUNT_ID END AS PARTNER_TRAVEL_BOX_ACCOUNT_ID,
CASE WHEN PARTNER_TRAVEL_BOX_ACCOUNT_NAME='' then NULL ELSE PARTNER_TRAVEL_BOX_ACCOUNT_NAME END AS PARTNER_TRAVEL_BOX_ACCOUNT_NAME,
CASE WHEN PARTNER_TRAVEL_BOX_ACCOUNT_PHONE_NUMBER='' then NULL ELSE PARTNER_TRAVEL_BOX_ACCOUNT_PHONE_NUMBER END AS PARTNER_TRAVEL_BOX_ACCOUNT_PHONE_NUMBER,
CASE WHEN REGION_CODE='' then NULL ELSE REGION_CODE END AS REGION_CODE,
CASE WHEN REGION_NAME='' then NULL ELSE REGION_NAME END AS REGION_NAME
from (select 
  distinct out2.accountid  as PARTNER_CRM_ACCOUNT_ID,
  out2.accountnumber as PARTNER_CRM_ACCOUNT_Number,
  initcap(out2.name) as PARTNER_CRM_ACCOUNT_NAME,
  lower(out2.emailaddress1) as PARTNER_CRM_ACCOUNT_EMAIL,
  out2.telephone1 as PARTNER_CRM_ACCOUNT_PHONE_NUMBER,
  out2.dot_segmentid as PARTNER_CRM_SEG_CODE,
  out2.dot_seg_name as PARTNER_CRM_SEG_NAME,
  out2.dot_subsegmentid as PARTNER_CRM_SUB_SEG_CODE,
  out2.dot_sub_seg_name_lkp as PARTNER_CRM_SUB_SEG_NAME,
  out2.yc_isehdmc as EH_DMC_FLAG,
  row2.AccountAk as PARTNER_OMNI_ACCOUNT_ID,
  case when ISNULL(row2.FirstName_1) or row2.FirstName_1 = "" or row2.FirstName_1 = "null" then concat(row2.FirstName_1," ",row2.SurName_1) else row2.FirstName_1 end as PARTNER_OMNI_ACCOUNT_NAME,
  row2.EmailAddress1_1 as PARTNER_OMNI_ACCOUNT_EMAIL,
  row2.MobilePhone_1 as PARTNER_OMNI_ACCOUNT_PHONE_NUMBER,
  row2.DmgCategoryCode as PARTNER_OMNI_ACCOUNT_CAT_DMG_CODE,
  crm_omni_categories.fe_name as PARTNER_OMNI_ACCOUNT_CAT_DMG_NAME,
  "" as PARTNER_TRAVEL_BOX_ACCOUNT_ID,
  "" as PARTNER_TRAVEL_BOX_ACCOUNT_NAME,
  "" as PARTNER_TRAVEL_BOX_ACCOUNT_EMAIL_ADDRESS,
  "" as PARTNER_TRAVEL_BOX_ACCOUNT_PHONE_NUMBER,
  "" as PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_CODE,
  "" as PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_NAME,
  "" as KEY_ACCOUNT_FLAG,
  "" as ACCOUNT_CATEGORY,
  "" as PARTNER_OMNI_YAS_ARENA_ACCOUNT_ID,
  "" as PARTNER_OMNI_YAS_ARENA_ACCOUNT_NAME,
  "" as PARTNER_OMNI_YAS_ARENA_ACCOUNT_EMAIL_ADDRESS, 
  "" as PARTNER_OMNI_YAS_ARENA_ACCOUNT_PHONE_NUMBER, 
  "" as PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_CODE,
  "" as PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_NAME,
  "" as PARTNER_ORACLE_ACCOUNT_ID,
  out2.parentaccountid  as PARENT_PARTNER_CRM_ACCOUNT_ID,
  out2._parentaccountid_value_lkp as PARENT_PARTNER_CRM_ACCOUNT_NAME,
  cast(row2.AccountId_1 as decimal(38,0)) as ACCOUNT_ID,
  row2.DmgCategoryCode  as OMNI_ACCOUNT_CAT_DMG_CODE,
  row2.Description  as OMNI_ACCOUNT_CAT_DMG_DESCRIPTION, 
  case when ISNULL(row2.RecursiveName) or row2.RecursiveName ="" or row2.RecursiveName="null" then "" else replace(row2.RecursiveName,"?", ">>") end as OMNI_ACCOUNT_CAT_DMG_RECURSIVE_NAME,
  row2.DmgParentId  as OMNI_ACCOUNT_CAT_DMG_PARENT,
  row2.DmgCategoryCode  as OMNI_ACCOUNT_PARENT_CAT_DMG_CODE,
  row2.Description  as OMNI_ACCOUNT_PARENT_CAT_DMG_DESCRIPTION,
  row2.RecursiveName  as OMNI_ACCOUNT_PARENT_CAT_DMG_RECURSIVE_NAME,
  out2.ACTIVE_FLAG  as ACTIVE_FLAG,
  out2.CONTRACT_STATUS  as CONTRACT_STATUS,
  cast(from_unixtime(out2.CRM_LAST_MODIFIED_DATE) as timestamp) as CRM_LAST_MODIFIED_DATE,
  out2.COUNTRY_CODE  as COUNTRY_CODE,
  out2.COUNTRY_NAME  as COUNTRY_NAME,
  out2.REGION_CODE AS REGION_CODE,
  out2.REGION_NAME  as REGION_NAME
--   ,ROW_NUMBER() OVER (
-- 			PARTITION BY out2.accountid ORDER BY CASE 
-- 					WHEN out2.ACTIVE_FLAG = 'Active'
-- 						THEN 1
-- 					ELSE 2
-- 					END
-- 				,out2.CRM_LAST_MODIFIED_DATE DESC
-- 			) rn
  from account_segment_map out2 left outer join account_dmg_map row2 on  out2.yc_omniaccountid=row2.AccountAk 
  left outer join fe_omnidmgcategories crm_omni_categories on out2.fe_omnidmgcategory = crm_omni_categories.fe_omnidmgcategoryid) T
--   WHERE rn = 1
  """)

# COMMAND ----------

# DBTITLE 1,selecting only 'Active' PARTNER_OMNI_ACCOUNT_ID's
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, when, monotonically_increasing_id

window_spec = Window.partitionBy(
    when(col("PARTNER_OMNI_ACCOUNT_ID").isNull(),
         monotonically_increasing_id()
    ).otherwise(col("PARTNER_OMNI_ACCOUNT_ID"))
).orderBy(
    when(col("ACTIVE_FLAG") == "Active", 1).otherwise(2)
)

b2b_partner_filtered_df = (
    b2b_partner_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)


# COMMAND ----------

from pyspark.sql.functions import lower, trim, regexp_replace, lit

b2b_partner_final_df = b2b_partner_filtered_df.select(
    "PARTNER_CRM_ACCOUNT_ID",
    "ACCOUNT_CATEGORY",
    "ACCOUNT_ID",
    "ACTIVE_FLAG",
    "CONTRACT_STATUS",
    "COUNTRY_CODE",
    "COUNTRY_NAME",
    "CRM_LAST_MODIFIED_DATE",
    "EH_DMC_FLAG",
    "KEY_ACCOUNT_FLAG",
    "OMNI_ACCOUNT_CAT_DMG_CODE",
    "OMNI_ACCOUNT_CAT_DMG_DESCRIPTION",
    "OMNI_ACCOUNT_CAT_DMG_PARENT",
    "OMNI_ACCOUNT_CAT_DMG_RECURSIVE_NAME",
    "OMNI_ACCOUNT_PARENT_CAT_DMG_CODE",
    "OMNI_ACCOUNT_PARENT_CAT_DMG_DESCRIPTION",
    "OMNI_ACCOUNT_PARENT_CAT_DMG_RECURSIVE_NAME",
    "PARENT_PARTNER_CRM_ACCOUNT_ID",
    "PARENT_PARTNER_CRM_ACCOUNT_NAME",
    lower(trim(regexp_replace("PARTNER_CRM_ACCOUNT_EMAIL", ";", ""))).alias("PARTNER_CRM_ACCOUNT_EMAIL"),
    "PARTNER_CRM_ACCOUNT_NAME",
    "PARTNER_CRM_ACCOUNT_NUMBER",
    "PARTNER_CRM_ACCOUNT_PHONE_NUMBER",
    "PARTNER_CRM_SEG_CODE",
    "PARTNER_CRM_SEG_NAME",
    "PARTNER_CRM_SUB_SEG_CODE",
    "PARTNER_CRM_SUB_SEG_NAME",
    "PARTNER_OMNI_ACCOUNT_CAT_DMG_CODE",
    "PARTNER_OMNI_ACCOUNT_CAT_DMG_NAME",
    lower(trim(regexp_replace("PARTNER_OMNI_ACCOUNT_EMAIL", ";", ""))).alias("PARTNER_OMNI_ACCOUNT_EMAIL"),
    "PARTNER_OMNI_ACCOUNT_ID",
    "PARTNER_OMNI_ACCOUNT_NAME",
    "PARTNER_OMNI_ACCOUNT_PHONE_NUMBER",
    "PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_CODE",
    "PARTNER_OMNI_YAS_ARENA_ACCOUNT_CAT_DMG_NAME",
    lower(trim(regexp_replace("PARTNER_OMNI_YAS_ARENA_ACCOUNT_EMAIL_ADDRESS", ";", ""))).alias("PARTNER_OMNI_YAS_ARENA_ACCOUNT_EMAIL_ADDRESS"),
    "PARTNER_OMNI_YAS_ARENA_ACCOUNT_ID",
    "PARTNER_OMNI_YAS_ARENA_ACCOUNT_NAME",
    "PARTNER_OMNI_YAS_ARENA_ACCOUNT_PHONE_NUMBER",
    "PARTNER_ORACLE_ACCOUNT_ID",
    "PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_CODE",
    "PARTNER_TRAVEL_BOX_ACCOUNT_CAT_DMG_NAME",
    lower(trim(regexp_replace("PARTNER_TRAVEL_BOX_ACCOUNT_EMAIL_ADDRESS", ";", ""))).alias("PARTNER_TRAVEL_BOX_ACCOUNT_EMAIL_ADDRESS"),
    "PARTNER_TRAVEL_BOX_ACCOUNT_ID",
    "PARTNER_TRAVEL_BOX_ACCOUNT_NAME",
    "PARTNER_TRAVEL_BOX_ACCOUNT_PHONE_NUMBER",
    "REGION_CODE",
    "REGION_NAME"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Schema Casting

# COMMAND ----------

b2b_partner_final_df = apply_schema_casting(b2b_partner_final_df, dim_b2b_partner_schema)
# Remove records where PARTNER_OMNI_ACCOUNT_ID is null
b2b_partner_final_df = b2b_partner_final_df.filter(F.col("PARTNER_OMNI_ACCOUNT_ID").isNotNull())

# COMMAND ----------

from pyspark.sql import functions as F

# Define the primary key column(s)
# "PARTNER_CRM_ACCOUNT_ID" is the primary key, since we are using "PARTNER_OMNI_ACCOUNT_ID" in joinings, so checking duplicate  check on "PARTNER_OMNI_ACCOUNT_ID".
primary_keys = ["PARTNER_OMNI_ACCOUNT_ID"]

# Build NOT NULL condition dynamically (supports composite keys)
not_null_condition = F.lit(True)
for pk in primary_keys:
    not_null_condition = not_null_condition & F.col(pk).isNotNull()

# Find duplicates ONLY for non-null keys
duplicate_df = (
    b2b_partner_final_df
    .filter(not_null_condition)
    .groupBy(primary_keys)
    .count()
    .filter(F.col("count") > 1)
)

# Check if duplicates exist and raise an exception
if duplicate_df.count() > 0:
    display(duplicate_df)  # Optional
    raise Exception(f"Duplicates found in b2b_partner_final_df for NON-NULL keys {primary_keys}. Failing notebook.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the Clean Final Data into the Silve Table
# MAGIC

# COMMAND ----------

# Build DDL string from schema dict
dim_b2b_partner_schema_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in dim_b2b_partner_schema.items()
)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(dim_b2b_partner_schema_ddl)
    if compare_schema_lenient(b2b_partner_final_df.schema, ddl_struct):
        print("Schema matches!")
        b2b_partner_final_df.write.format("delta")\
                        .mode("overwrite")\
                        .option("overwriteSchema", "true")\
                        .saveAsTable(slv_table)
    else:
        print("Schema mismatch!")
        # displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        # displayHTML(f"<b>Actual:</b><br><pre>{b2b_partner_final_df.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise Exception(f"FAIL: {str(e)}")
