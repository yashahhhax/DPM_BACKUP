# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import expr, lit
import pyodbc

# COMMAND ----------

# DBTITLE 1,Load shared utilities
# MAGIC %run ../../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,Define notebook widget parameters
dbutils.widgets.text("env","")                          # Environment identifier (dev/uat/prod)
dbutils.widgets.text("secret_scope","")                  # Azure Key Vault secret scope name
dbutils.widgets.text("crm_sql_server_name","")           # Azure SQL Server hostname
dbutils.widgets.text("crm_sql_database","")              # Target SQL database name
dbutils.widgets.text("crm_sql_database_username","")     # SQL auth username
dbutils.widgets.text("crm_sql_database_secret_name","")      # Key Vault key name for SQL password
dbutils.widgets.text("days_to_load","90")      # Key Vault key name for SQL password

# COMMAND ----------

# DBTITLE 1,Extract parameters into variables
try:
    notebook_vars = [
        'env',
        'secret_scope',
        'crm_sql_server_name',
        'crm_sql_database',
        'crm_sql_database_username',
        'crm_sql_database_secret_name',
        'days_to_load'
    ]
    env, akv_scope, jdbcHostname, jdbcDatabase, jdbcUsername, passwordkey, days_to_load = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Connection to SQL Database

# COMMAND ----------

# DBTITLE 1,Setup JDBC connection
jdbcPassword   = dbutils.secrets.get(akv_scope,  passwordkey)

connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

jdbcSqlConnStr = f"jdbc:sqlserver://{jdbcHostname}.database.windows.net:1433;database={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

if not days_to_load or days_to_load.strip() == "":
    days_to_load = "14"

# COMMAND ----------

conn = pyodbc.connect(
f"DRIVER={{ODBC Driver 17 for SQL Server}};"
f"SERVER={jdbcHostname}.database.windows.net;"
f"DATABASE={jdbcDatabase};"
f"UID={jdbcUsername};"
f"PWD={jdbcPassword};"
"Encrypt=yes;"
"TrustServerCertificate=no;"
"Connection Timeout=30;")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert C360 **KPIs**

# COMMAND ----------

kpi_data_df = spark.sql(f"""
select 
base_tb.REPORT_DATE as KPI_DATE
,scv_master.mdm_id as MDM_ID
,base_tb.email_id as email_address
,base_tb.yas_id
,base_tb.FIRST_NAME
,base_tb.LAST_NAME
,base_tb.MOBILE_NUM
,base_tb.dob as DATE_OF_BIRTH
,base_tb.GENDER
,base_tb.country_of_residence_name as COUNTRY
,base_tb.NATIONALITY_Name as nationality
,base_tb.kpi_json.TOTAL_REVENUE_GENERATED_LAST_12_MONTHS as TOTAL_REVENUE_GENERATED_LAST_12_MONTHS
,cast(NULL AS STRING) as CHURN_STATUS
,base_tb.kpi_json.LAST_CHECKIN_TIMESTAMP as LAST_CHECKIN_TIMESTAMP
,base_tb.kpi_json.LATEST_PARK_VISITED as LATEST_PARK_VISITED
from transformation_{env}.plt_customer360.tbvw_customer_kpi_by_email base_tb
INNER JOIN transformation_{env}.plt_customer360.scv_account_master scv_master
        ON base_tb.email_id = scv_master.email_id
        AND scv_master.source_name = 't_mdm_guest'
""")

kpi_data_df.write.jdbc(jdbcSqlConnStr, "customer_kpi", "overwrite", connectionProperties)
print("Data loaded successfully for customer_kpi stage with record count:", kpi_data_df.count())


# COMMAND ----------

conn.execute(f"""TRUNCATE TABLE crm.customer_kpi;""")
conn.execute(f"""INSERT INTO crm.customer_kpi
                  (        
                                     KPI_DATE
                                    ,MDM_ID
                                    ,email_address
                                    ,yas_id
                                    ,FIRST_NAME
                                    ,LAST_NAME
                                    ,MOBILE_NUM
                                    ,DATE_OF_BIRTH
                                    ,GENDER
                                    ,COUNTRY
                                    ,NATIONALITY
                                    ,TOTAL_REVENUE_GENERATED_LAST_12_MONTHS
                                    ,CHURN_STATUS
                                    ,LAST_CHECKIN_TIMESTAMP
                                    ,LATEST_PARK_VISITED
                                    ,FilteredMobNumber
                  )                     
                  select             KPI_DATE
                                    ,CAST(MDM_id AS varchar(255)) AS MDM_ID
                                    ,email_address
                                    ,yas_id
                                    ,FIRST_NAME
                                    ,LAST_NAME                             
                                    ,MOBILE_NUM
                                    ,DATE_OF_BIRTH
                                    ,GENDER
                                    ,COUNTRY
                                    ,NATIONALITY
                                    ,TRY_CAST(TOTAL_REVENUE_GENERATED_LAST_12_MONTHS AS DECIMAL(18,2)) AS TOTAL_REVENUE_GENERATED_LAST_12_MONTHS
                                    ,CHURN_STATUS
                                    ,LAST_CHECKIN_TIMESTAMP
                                    ,LATEST_PARK_VISITED
                                    ,CAST(MOBILE_NUM AS varchar(255)) as FilteredMobNumber
                  from dbo.customer_kpi with (nolock) """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert C360 Transactions

# COMMAND ----------

transactions_data_df = spark.sql(f"""
select 
cast(scv_master.mdm_id as STRING) as MDM_ID
,base_tb.email_entity_identifier as email_address
,base_tb.scv_entity_identifier.yas_id as yas_id
,base_tb.selling_entity
,base_tb.PRODUCT_NAME
,base_tb.PRODUCT_CATEGORY
,base_tb.PRODUCT_SUBCATEGORY
,base_tb.SALE_TIMESTAMP
,base_tb.TICKET_AGE_CATEGORY
,base_tb.QUANTITY
,base_tb.TICKET_TIER
,base_tb.ANNUAL_PASS
,base_tb.ANNUAL_PASS_DMG
,base_tb.TICKET_VALID_FROM
,base_tb.TICKET_VALID_TO
,base_tb.GROSS_SALE_AMOUNT 
from transformation_{env}.plt_customer360.vw_transactions base_tb
INNER JOIN transformation_{env}.plt_customer360.scv_account_master scv_master
        ON base_tb.email_entity_identifier = scv_master.email_id
        AND scv_master.source_name = 't_mdm_guest'
where base_tb.SALE_TIMESTAMP >= date_sub(current_date(), {days_to_load})
and base_tb.email_entity_identifier is not null
and scv_master.MDM_ID is not null
""")

transactions_data_df.write.jdbc(jdbcSqlConnStr, "transactional", "overwrite", connectionProperties)
print("Data loaded successfully for transactional stage with record count:", transactions_data_df.count())

# COMMAND ----------

conn.execute(f"""INSERT INTO  [crm].[transactional]
                  (MDM_ID
                  ,email_address
                  ,yas_id
                  ,selling_entity
                  ,PRODUCT_NAME
                  ,PRODUCT_CATEGORY
                  ,PRODUCT_SUBCATEGORY
                  ,SALE_TIMESTAMP
                  ,TICKET_AGE_CATEGORY
                  ,QUANTITY
                  ,TICKET_TIER
                  ,ANNUAL_PASS
                  ,ANNUAL_PASS_DMG
                  ,TICKET_VALID_FROM
                  ,TICKET_VALID_TO
                  ,GROSS_SALE_AMOUNT)

            SELECT [MDM_ID]
                  ,[email_address]
                  ,[yas_id]
                  ,[selling_entity]
                  ,[PRODUCT_NAME]
                  ,[PRODUCT_CATEGORY]
                  ,[PRODUCT_SUBCATEGORY]
                  ,[SALE_TIMESTAMP]
                  ,[TICKET_AGE_CATEGORY]
                  ,[QUANTITY]
                  ,[TICKET_TIER]
                  ,[ANNUAL_PASS]
                  ,[ANNUAL_PASS_DMG]
                  ,[TICKET_VALID_FROM]
                  ,[TICKET_VALID_TO]
                  ,[GROSS_SALE_AMOUNT]
            FROM [dbo].[transactional] a
            where SALE_TIMESTAMP > '2024-02-01'
            and not exists (select 1 from crm.[transactional] b where a.MDM_ID=b.MDM_ID
                                    and a.email_address=b.email_address
                                          and a.selling_entity=b.selling_entity
                                          and a.PRODUCT_NAME=b.PRODUCT_NAME
                                          and CONVERT(date, a.SALE_TIMESTAMP)=CONVERT(date, b.SALE_TIMESTAMP))""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert C360 Location Intelligence

# COMMAND ----------

locations_data_df = spark.sql(f"""
select 
 scv_master.mdm_id as MDM_ID
,base_tb.email_entity_identifier as email_address
,scv_master.yas_id as yas_id
,scv_master.first_name
,scv_master.Last_name
,base_tb.ASSET_NAME
,base_tb.PRODUCT_NAME
,base_tb.CATEGORY
,base_tb.CHECKIN_TIMESTAMP 
from transformation_{env}.plt_customer360.vw_location_intelligence base_tb
INNER JOIN transformation_{env}.plt_customer360.scv_account_master scv_master
        ON base_tb.email_entity_identifier = scv_master.email_id
        AND scv_master.source_name = 't_mdm_guest'
where  CHECKIN_TIMESTAMP >= date_sub(current_date(), {days_to_load})
and base_tb.email_entity_identifier is not null
and scv_master.MDM_ID is not null;""")

locations_data_df.write.jdbc(jdbcSqlConnStr, "location_intellegence", "overwrite", connectionProperties)
print("Data loaded successfully for location_intellegence stage with record count:", locations_data_df.count())

# COMMAND ----------

conn.execute(f"""INSERT INTO [crm].[location_intellegence]
                    (
                         [MDM_ID]
                        ,[email_address]
                        ,[yas_id]
                        ,[first_name]
                        ,[Last_name]
                        ,[ASSET_NAME]
                        ,[PRODUCT_NAME]
                        ,[CATEGORY]
                        ,[CHECKIN_TIMESTAMP]
                    )
                    SELECT	  
                         MDM_ID
							          ,email_address
                        ,yas_id
                        ,first_name
                        ,Last_name
                        ,ASSET_NAME
                        ,PRODUCT_NAME
                        ,CATEGORY
                        ,CHECKIN_TIMESTAMP
			              FROM dbo.location_intellegence a
					          WHERE
						              a.CHECKIN_TIMESTAMP >= '2024-02-01'
						          and a.asset_name is not null
						          AND NOT EXISTS (
							                          SELECT 1
							                          FROM [crm].location_intellegence b
							                          WHERE b.asset_name is not null
								                            and a.MDM_ID = b.MDM_ID
								                            AND a.email_address = b.email_address
								                            AND CONVERT(date, a.CHECKIN_TIMESTAMP) = CONVERT(date, b.CHECKIN_TIMESTAMP)
								                            AND a.ASSET_NAME = b.ASSET_NAME
						                          );""")

# COMMAND ----------

conn.close()
