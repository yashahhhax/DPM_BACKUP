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
        'crm_sql_server_name',
        'crm_sql_database',
        'crm_sql_database_username',
        'crm_sql_database_secret_name',
        'days_to_load'
    ]
    env, jdbcHostname, jdbcDatabase, jdbcUsername, passwordkey, days_to_load = get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Connection to SQL Database

# COMMAND ----------

# DBTITLE 1,Setup JDBC connection
secret_scope = f'kv-mx-data-adf-inges-{env}'
jdbcPassword   = dbutils.secrets.get(secret_scope,  passwordkey)

connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

jdbcSqlConnStr = f"jdbc:sqlserver://{jdbcHostname}.database.windows.net:1433;database={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"
print("Connection String:",jdbcSqlConnStr)

if not days_to_load or days_to_load.strip() == "":
    days_to_load = "90"


# COMMAND ----------

conn = pyodbc.connect(
f"DRIVER={{ODBC Driver 17 for SQL Server}};"
f"SERVER={jdbcHostname}.database.windows.net;"
f"DATABASE={jdbcDatabase};"
f"UID={jdbcUsername};"
f"PWD={jdbcPassword};"
"Encrypt=yes;"
"TrustServerCertificate=yes;"
"Connection Timeout=600;")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare C360 HVG Export

# COMMAND ----------

hvg_data_df = spark.sql(f"""
select 
email_id as email
,mobile_num
,IFF(try_cast(kpi_json.TOTAL_REVENUE_GENERATED_LAST_12_MONTHS as double) >= 5000 ,True,False) as hvg_flag
,kpi_json.LAST_CHECKIN_TIMESTAMP as lastVisitedDate
,kpi_json.LATEST_PARK_VISITED as lastParkVisited
,EXISTS (
    SELECT 1 
    FROM transformation_dev.plt_customer360.annual_pass ap
    WHERE ap.entity_identifiers.email_id = email_id
    and status = 'ACTIVE'
) AS isannualPass
,first_name as firstName
,last_name as lastName
,'{jdbcUsername}' as createdBy
,current_timestamp() as CreatedDate
,cast(NULL as timestamp) as updatedBy
,cast(NULL as timestamp) as UpdatedDate
from transformation_dev.plt_customer360.tbvw_customer_kpi_by_email
where 1=1 
and report_date is not null
""")

print("Record count:", hvg_data_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert into SCV Db

# COMMAND ----------

hvg_data_df.write.jdbc(jdbcSqlConnStr, "HIGH_VALUE_GUEST", "overwrite", connectionProperties)
print("Data loaded successfully for HIGH_VALUE_GUEST with record count:", hvg_data_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Insertion

# COMMAND ----------

cursor = conn.cursor()
cursor.execute("""
SELECT COUNT(*) AS record_count, MAX(CreatedDate) AS max_createdDate
FROM dbo.HIGH_VALUE_GUEST
""")
result = cursor.fetchone()
record_count, max_createdDate = result
print("Record count:", record_count)
print("Max CreatedDate:", max_createdDate)

# COMMAND ----------

cursor.close()
conn.close()
