# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC
# MAGIC **The `crm_account_contact_rfp` table serves as a consolidated platinum-level dataset for account and contact relationships, integrating data from CRM bronze tables. It enforces schema consistency and supports full overwrite load strategy.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. Source Tables:
# MAGIC
# MAGIC | Schema  | Table                           |
# MAGIC | ------- | ------------------------------- |
# MAGIC | brz_crm | vw_crm_account                  |
# MAGIC | brz_crm | vw_crm_stringmap                |
# MAGIC | brz_crm | vw_crm_businessunit             |
# MAGIC | brz_crm | vw_crm_contact                  |
# MAGIC | brz_crm | vw_crm_systemuser               |
# MAGIC | brz_crm | vw_crm_yc_accountmanagerhistory |
# MAGIC | brz_crm | vw_crm_dot_clientcontract       |
# MAGIC | brz_crm | vw_crm_dot_segment              |
# MAGIC | brz_crm | vw_crm_dot_country              |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Target Platinum table:
# MAGIC
# MAGIC **transformation_dev.plt_customer360.crm_account_contact_rfp**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Load Strategy:
# MAGIC
# MAGIC **Full Load (Overwrite on each run using mode("overwrite"))**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Technical Details:
# MAGIC
# MAGIC | Attribute              | Details                                        |
# MAGIC | ---------------------- | ---------------------------------------------- |
# MAGIC | **Primary Key**        | account_id + contact_id                        |
# MAGIC | **Business Key(s)**    | scv_account_sk                                 |
# MAGIC | **Schema Enforcement** | Yes (DDL validation + overwriteSchema enabled) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. Notes:
# MAGIC
# MAGIC * Full refresh performed on every run
# MAGIC * Ensures latest snapshot of account-contact relationships
# MAGIC * Schema evolution handled using overwriteSchema
# MAGIC

# COMMAND ----------

# DBTITLE 1,Including schema config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,including common function
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# MAGIC %run ../../config/view_queries

# COMMAND ----------

from datetime import datetime, timedelta, date
from typing import List
import pandas as pd
import numpy as np
import pyspark.sql as SQL
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, col, add_months, current_date, expr, trim, from_json
from pyspark.sql.types import StructType, StringType, MapType
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
import itertools

# COMMAND ----------

try:
    notebook_vars = [
        'env','current_job_run_id'
    ]
    env,current_job_run_id= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,crm_schema
crm_schema = f"transformation_{env}.brz_crm"
tgt_db = f"transformation_{env}.plt_customer360"

# Source table names
vw_crm_account = f"{crm_schema}.vw_crm_account"  
vw_crm_stringmap = f"{crm_schema}.vw_crm_stringmap"
vw_crm_businessunit = f"{crm_schema}.vw_crm_businessunit"
vw_crm_contact = f"{crm_schema}.vw_crm_contact"
vw_crm_systemuser = f"{crm_schema}.vw_crm_systemuser"
vw_crm_yc_accountmanagerhistory = f"{crm_schema}.vw_crm_yc_accountmanagerhistory"
vw_crm_dot_clientcontract = f"{crm_schema}.vw_crm_dot_clientcontract"
vw_crm_dot_segment = f"{crm_schema}.vw_crm_dot_segment"
vw_crm_dot_country = f"{crm_schema}.vw_crm_dot_country"

src_scv   = f"{tgt_db}.scv_account_master"


# Target table name
target_table = f"transformation_{env}.plt_customer360.crm_account_contacts_rfp"


current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table: {vw_crm_account}")
print(f"Source table: {vw_crm_stringmap}")
print(f"Source table: {vw_crm_businessunit}")
print(f"Source table: {vw_crm_contact}")
print(f"Source tanle: {vw_crm_systemuser}")
print(f"Source table: {vw_crm_yc_accountmanagerhistory}")
print(f"Source table: {vw_crm_dot_clientcontract}" )
print(f"Source table: {vw_crm_dot_segment}")
print(f"Source table: {vw_crm_dot_country}")
print(f"Target table: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")

# COMMAND ----------

# def clean_crm_account_return_df(source_table: str) -> DataFrame:
#     """
#     Cleans CRM table:
#     - Removes duplicates (excluding adf_load_time)
#     - Drops fully null rows
#     - Trims all string columns
#     Optimized for large tables: minimizes shuffles, uses vectorized operations.
#     """

#     df = spark.table(source_table)

#     columns_to_consider = [c for c in df.columns if c != "adf_load_time"]

#     # Vectorized trim for all string columns
#     string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
#     trim_exprs = [trim(col(c)).alias(c) if c in string_cols else col(c) for c in df.columns]
#     df = df.select(*trim_exprs)

#     # Drop fully null rows and duplicates in one go to minimize shuffles
#     df = df.dropna(how="all", subset=columns_to_consider).dropDuplicates(columns_to_consider)

#     return df


# COMMAND ----------

# DBTITLE 1,clean_crm_account_return_df
def clean_crm_return_df(source_table: str, drop_null_rows: bool = False) -> DataFrame:
    """
    Cleans CRM table:
    - Trims all string columns
    - Optionally drops fully null rows (when drop_null_rows=True)
    - Removes duplicates (excluding adf_load_time)
    Optimized for large tables: minimizes shuffles, uses vectorized operations.

    Parameters:
    source_table (str): Fully qualified table name to read.
    drop_null_rows (bool): If True, drops rows where all columns (except adf_load_time) are null.
                           Default: False.

    Returns:
    DataFrame: Cleaned PySpark DataFrame.
    """

    df = spark.table(source_table)

    columns_to_consider = [c for c in df.columns if c != "adf_load_time"]

    # Vectorized trim for all string columns
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    trim_exprs = [trim(col(c)).alias(c) if c in string_cols else col(c) for c in df.columns]
    df = df.select(*trim_exprs)

    # Optionally drop fully null rows before dedup to minimize shuffle volume
    if drop_null_rows:
        df = df.dropna(how="all", subset=columns_to_consider)

    df = df.dropDuplicates(columns_to_consider)

    return df

# COMMAND ----------


# df_crm_account = clean_crm_account_return_df(vw_crm_account)
# df_crm_stringmap = clean_and_return_df(vw_crm_stringmap)
# df_crm_business_unit = clean_and_return_df(vw_crm_businessunit)
# df_crm_contacts = clean_and_return_df(vw_crm_contact)
# df_crm_system_user = clean_and_return_df(vw_crm_systemuser)
# df_crm_account_manager = clean_and_return_df(vw_crm_yc_accountmanagerhistory)
# df_crm_client_contracts = clean_and_return_df(vw_crm_dot_clientcontract)
# df_crm_dot_segment = clean_and_return_df(vw_crm_dot_segment)
# df_crm_dot_country = clean_and_return_df(vw_crm_dot_country)

# COMMAND ----------

# DBTITLE 1,clean_and_return_df
# Account table uses drop_null_rows=True (has known fully-null rows from ADF loads)
df_crm_account = clean_crm_return_df(vw_crm_account, drop_null_rows=True)

# Remaining tables: standard dedup + trim only
df_crm_stringmap = clean_crm_return_df(vw_crm_stringmap)
df_crm_business_unit = clean_crm_return_df(vw_crm_businessunit)
df_crm_contacts = clean_crm_return_df(vw_crm_contact)
df_crm_system_user = clean_crm_return_df(vw_crm_systemuser)
df_crm_account_manager = clean_crm_return_df(vw_crm_yc_accountmanagerhistory)
df_crm_client_contracts = clean_crm_return_df(vw_crm_dot_clientcontract)
df_crm_dot_segment = clean_crm_return_df(vw_crm_dot_segment)
df_crm_dot_country = clean_crm_return_df(vw_crm_dot_country)

# COMMAND ----------

# DBTITLE 1,percInColumnNotLike
def percInColumnNotLike(dataFrame: SQL.DataFrame, columnName: str, columnLike: str) -> float:
  df = dataFrame.where(~F.col(columnName).like(columnLike))
  perc_calc = df.count()/dataFrame.count()
  return perc_calc

def percInColumnForValue(dataFrame: SQL.DataFrame, columnName: str, columnValue: str) -> float:
  df = dataFrame.filter(F.col(columnName) == columnValue)
  perc_calc = df.count()/dataFrame.count()
  return perc_calc

def percInColumnForNull(dataFrame: SQL.DataFrame, columnName: str) -> float:
  df = dataFrame.filter(F.col(columnName).isNull())
  perc_calc = df.count()/dataFrame.count()
  return perc_calc

def percDupsInColumn(dataFrame: SQL.DataFrame, columnName: str) -> float:
  df = dataFrame.dropDuplicates([columnName])
  perc_calc = 1 - df.count()/dataFrame.count()
  return perc_calc

# COMMAND ----------

# DBTITLE 1,read_input_parameter
def read_input_parameter(key: str, default:str= "") -> str:
  """
  Description:
  Returns a widget value/ parameter passed by a workflow. In case the value is not provided, the function returns the given default. Feature is implemented to workaround the cascading updates on widget values configured on a user level (https://docs.databricks.com/notebooks/widgets.html#configure-widget-settings-1). This is not suitable for file writes.

  Parameters:
  key (str): Key describing the key of the parameter being passed or the name of the widget
  default (str): Return value in case of exceptions (e.g. when key is not provided). Defaults to ""

  Returns:
  Configuration as str. All widget configurations are passed as strings.
  """
  try:
    return dbutils.widgets.get(key)
  except:
    print(f"Parameter '{key}' not provided, reading from default")
    return default

# COMMAND ----------

# DBTITLE 1,remove_non_matches Function
def remove_non_matches(df_name:str, df:SQL.DataFrame, cols_to_match:List[str], pattern:str) -> SQL.DataFrame:
  """
  Description:
  Removes non-matching rows from a PySpark DataFrame based on a regular expression pattern.
  The filtering happens for all columns provided in the column list

  Parameters:
  df_name (str): The name of the DataFrame.
  df (pyspark.sql.DataFrame): The PySpark DataFrame to filter.
  cols_to_match (List[str]): A list of column names to apply the regular expression pattern to.
  pattern (str): A regular expression pattern to use for matching.

  Returns:
  pyspark.sql.DataFrame: A new PySpark DataFrame with rows removed that do not match the given pattern.

  Example:
  df = spark.read.csv('data.csv', header=True)
  filtered_df = remove_non_matches('my_df', df, ['col1', 'col2'], 'pattern')
  
  This will remove all rows from my_df dataframe where any of the specified columns don't match the regex pattern
  """
  # rows_before = df.count()
  for select_col in cols_to_match:
    df = df.filter(F.col(select_col).rlike(pattern))
  # rows_after = df.count()
  # print(f"{df_name}: Rows before: {rows_before}, Rows after: {rows_after}, Rows_removed:{rows_before-rows_after}")
  return df
  

# COMMAND ----------

# DBTITLE 1,make_sql_table_unique Function
# default filter means no filter as its always True (1=1)
def make_sql_table_unique(table:str, unique_col:str,
                          order_by_col1:str, order_by_col2:str= None,
                          ordering_col1:str= 'desc', ordering_col2:str= 'desc', 
                          filters:str= '1=1', overwrite_table:bool= True) -> SQL.DataFrame:
  """
  Description:
  Returns a DataFrame with unique values for a given column.

  Parameters:
  table (str): The SQL temp view name for the input table to be de-duplicated
  unique_col (str): The name of the column with the unique values.
  order_by_col1 (str): The name of the column to order by.
  order_by_col2 (str): The name of the second column to order by (default: None).
  ordering_col1 (str): The ordering of the first column (default: 'desc').
  ordering_col2 (str): The ordering of the second column (default: 'desc').
  filters (str): SQL WHERE clause with filtering conditions for the input table (default: '1=1' i.e. no filtering).
  overwrite_table (bool): If True, overwrite the input temp table view with the transformed unique data, otherwise create a new temp table view with suffix '_unique' (default: True).

  Returns:
  pyspark.sql.DataFrame: A DataFrame with unique values for specified column.

  Example:
  >>> input_df = spark.createDataFrame([(1, 'A', '2022-01-01'), (2, 'B', '2022-01-02'), 
                                        (3, 'A', '2022-01-03'), (4, 'C', '2022-01-04'), 
                                        (5, 'D', '2022-01-05'), (6, 'D', '2022-01-06'), 
                                        (7, 'A', '2022-01-07')], ['id', 'value', 'date'])
  >>> input_df.createOrReplaceTempView('sql_temp_view')
  >>> output_df = make_sql_table_unique('sql_temp_view', 'value', 'date', 'id')
  successfully created table view sql_temp_view and 3 duplicate rows removed (ordering:desc,desc, filters: 1=1)
  """
  
  # Single scan to get both total count and distinct count
  counts = spark.sql(f"select count(*) as total, count(distinct {unique_col}) as distinct_cnt from {table} where {filters}").collect()[0]
  previous_rows = counts['total']
  dup_count = previous_rows - counts['distinct_cnt']

  if dup_count == 0:
    query_df = spark.sql(f"select * from {table} where {filters}")
    if overwrite_table:
      query_df.createOrReplaceTempView(f"{table}")
      print(f"no duplicates found, table view {table} unchanged (filters: {filters})")
    else:
      query_df.createOrReplaceTempView(f"{table}_unique")
      print(f"no duplicates found, table view {table}_unique created (filters: {filters})")
    return query_df

  query = f"""
    with a_data as
    (select *,
          row_number() OVER (PARTITION BY {unique_col} ORDER BY {order_by_col1} {ordering_col1}
                        {', '+order_by_col2 if order_by_col2 is not None else ''} {ordering_col2 if order_by_col2 is not None else ''}) as sno
    from {table}
    where {filters})

  select * except (sno)
  from a_data
  where sno=1"""
  
  query_df= spark.sql(query)
  if overwrite_table==True:
    query_df.createOrReplaceTempView(f"{table}")
    # Use known counts to avoid an additional scan: unique rows = distinct count
    updated_rows = counts['distinct_cnt']
    print(f"successfully replaced table view {table} and {previous_rows-updated_rows} duplicate rows removed (ordering:{ordering_col1}{','+ordering_col2 if order_by_col2 is not None else ''}, filters: {filters})")
  else:
    query_df.createOrReplaceTempView(f"{table}_unique")
    updated_rows = counts['distinct_cnt']
    print(f"successfully created table view {table}_unique and {previous_rows-updated_rows} duplicate rows removed (ordering:{ordering_col1}{','+ordering_col2 if order_by_col2 is not None else ''}, filters: {filters})")
  return query_df

# COMMAND ----------

# DBTITLE 1,melt_ps_dataframe function
def melt_ps_dataframe(
      df: SQL.DataFrame,
      id_vars: List[str], value_vars: List[str],
      var_name:str= "variable", value_name:str= "value") -> SQL.DataFrame:
  """
  Description:
  This function transforms the given input Pyspark dataframe from wide format to long format using the id_vars and value_vars passed as parameters

  Parameters:
  df (SQL.DataFrame): the SQL DataFrame to be transformed from wide to long format.
  id_vars (List[str]): the columns to be kept as identifier variables as a list
  value_vars (List[str]): the columns to be melted as a list
  var_name (str): the name of the column in the output DataFrame to be used as a variable to hold the value_vars column names
  value_name (str): the name of the column in the output DataFrame to be used as a variable to hold the values from the value_vars columns

  Returns
  SQL.DataFrame : The SQL DataFrame in long format

  Example
  >>> df = spark.createDataFrame([(1, "John", "M", 50, 100),
                                  (2, "Jane", "F", 45, 200),
                                  (3, "Bob", "M", 52, 300),
                                  (4, "Alice", "F", 38, 400)],
                                  ["id", "name", "gender", "age", "salary"])
  >>> melted_df = melt_ps_dataframe(df, ["id", "name"], ["age", "salary"], "variable", "value")
  >>> print("Input DataFrame:")
  >>> df.show()
  >>> print("Melted DataFrame:")
  >>> melted_df.show()

  Output
  Input DataFrame:
  +---+-----+------+---+------+
  | id| name|gender|age|salary|
  +---+-----+------+---+------+
  |1  |John |M     | 50| 100  |
  |2  |Jane |F     | 45| 200  |
  |3  |Bob  |M     | 52| 300  |
  |4  |Alice|F     | 38| 400  |
  +---+-----+------+---+------+

  Melted DataFrame:
  +---+-----+--------+-----+
  | id| name|variable|value|
  +---+-----+--------+-----+
  |1  |John |age     | 50  |
  |1  |John |salary  | 100 |
  |2  |Jane |age     | 45  |
  |2  |Jane |salary  | 200 |
  |3  |Bob  |age     | 52  |
  |3  |Bob  |salary  | 300 |
  |4  |Alice|age     | 38  |
  |4  |Alice|salary  | 400 |
  +---+-----+--------+-----+
  """

  # Create an array of structs with all columns to be unpivoted
  # leaving just two non-identifier columns, ‘variable’ and ‘value’.
  _vars_and_vals = F.array(*(
      F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
      for c in value_vars))

  # Add to the DataFrame and explode
  _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

  # Select the required columns
  cols = id_vars + [
      F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
  ]

  return _tmp.select(*cols)

# COMMAND ----------

# DBTITLE 1,read_input_parameter
WRITE_MODE = read_input_parameter(key="write_mode", default = "False") == "True"
WRITE_PREFIX = read_input_parameter(key="write_prefix", default = "")
CRM_INPUT_DAY_FOLDER = read_input_parameter(key="crm_input_day_folder", default = datetime.today().strftime("%Y/%m/%d"))
#CRM_INPUT_DAY_FOLDER = '2024/01/31'
print(f'config: write_mode: {WRITE_MODE};, write_prefix: {WRITE_PREFIX};, crm_input_day_folder: {CRM_INPUT_DAY_FOLDER}')

# COMMAND ----------

# DBTITLE 1,REGEX_PATTERN
ACCOUNT_CONTACT_COLS = {
                      'primarycontactid': 'primary',
                      'dot_financecontactid': 'finance',
                      'dot_marketingcontactid': 'marketing',
                      'dot_operationscontactid': 'operations',
                      'dot_signercontactid': 'signer',
                      'flpm_generalmanagercontact': 'generalmanager',
                      'flpm_salesdirectorcontact': 'salesdirector',
                      'flpm_sponsorshipmanagerid': 'sponsorshipmanager',
                      'dot_contractingcontactid': 'contracting'
                      }

GUID_REGEX_PATTERN = '^' + '-'.join(['[a-zA-Z0-9]+']*5) + '$'
NAME_REGEX_PATTERN = '[a-zA-Z]+'   # should contain some letters (i.e. not only a number)

# COMMAND ----------

# DBTITLE 1,remove_non_matches
df_crm_contacts = remove_non_matches('crm_contacts', df_crm_contacts, ['contactid'], GUID_REGEX_PATTERN)
df_crm_account = remove_non_matches('crm_account', df_crm_account, ['accountid'], GUID_REGEX_PATTERN)
df_crm_account_manager = remove_non_matches('crm_account_manager', df_crm_account_manager, ['yc_accountid'], GUID_REGEX_PATTERN)
df_crm_client_contracts = remove_non_matches('crm_client_contracts', df_crm_client_contracts, ['dot_accountid'], GUID_REGEX_PATTERN)

df_crm_stringmap = remove_non_matches('crm_stringmap', df_crm_stringmap, [], GUID_REGEX_PATTERN)   #No pattern for string map data

# For lookup tables, applying patterns for lookup name values as well
df_crm_business_unit = remove_non_matches('crm_business_unit', df_crm_business_unit, ['businessunitid'], GUID_REGEX_PATTERN)
df_crm_business_unit = remove_non_matches('crm_business_unit', df_crm_business_unit, ['name'], NAME_REGEX_PATTERN)

df_crm_dot_segment = remove_non_matches('crm_dot_segment', df_crm_dot_segment, ['dot_segmentid'], GUID_REGEX_PATTERN)
df_crm_dot_segment = remove_non_matches('crm_dot_segment', df_crm_dot_segment, ['dot_name'], NAME_REGEX_PATTERN)

df_crm_dot_country = remove_non_matches('crm_dot_country', df_crm_dot_country, ['dot_countryid'], GUID_REGEX_PATTERN)
df_crm_dot_country = remove_non_matches('crm_dot_country', df_crm_dot_country, ['dot_name'], NAME_REGEX_PATTERN)

df_crm_system_user = remove_non_matches('crm_system_user', df_crm_system_user, ['systemuserid'], GUID_REGEX_PATTERN)
df_crm_system_user = remove_non_matches('crm_system_user', df_crm_system_user, ['fullname'], NAME_REGEX_PATTERN)

# COMMAND ----------

# DBTITLE 1,createOrReplaceTempView
df_crm_contacts.createOrReplaceTempView('crm_contacts')
df_crm_account.createOrReplaceTempView('crm_account')
df_crm_account_manager.createOrReplaceTempView('crm_account_manager')
df_crm_client_contracts.createOrReplaceTempView('crm_client_contracts')
df_crm_stringmap.createOrReplaceTempView('crm_stringmap')
df_crm_business_unit.createOrReplaceTempView('crm_business_unit')
df_crm_dot_country.createOrReplaceTempView('crm_dot_country')
df_crm_system_user.createOrReplaceTempView('crm_system_user')
df_crm_dot_segment.createOrReplaceTempView('crm_dot_segment')

# COMMAND ----------

# DBTITLE 1,make_sql_table_unique
df_crm_contacts = make_sql_table_unique('crm_contacts', unique_col='contactid', order_by_col1='modifiedon', order_by_col2='modifiedon')
df_crm_account = make_sql_table_unique('crm_account', unique_col='accountid', order_by_col1='createdon', order_by_col2='modifiedon')
df_crm_account_manager = make_sql_table_unique('crm_account_manager', unique_col='yc_accountmanagerhistoryid', order_by_col1='createdon', order_by_col2='modifiedon')
df_crm_client_contracts = make_sql_table_unique('crm_client_contracts', unique_col='dot_clientcontractid', order_by_col1='createdon', order_by_col2='modifiedon')

# Theres no date column available in string_map table to resolve duplicates, etc. so we take a random column here

df_crm_stringmap = make_sql_table_unique("crm_stringmap", unique_col="objecttypecode, attributename, attributevalue", order_by_col1="displayorder")

df_crm_business_unit = make_sql_table_unique('crm_business_unit', unique_col='businessunitid', order_by_col1='createdon', order_by_col2='modifiedon')
df_crm_dot_segment = make_sql_table_unique('crm_dot_segment', unique_col='dot_segmentid', order_by_col1='createdon', order_by_col2='modifiedon')
df_crm_dot_country = make_sql_table_unique('crm_dot_country', unique_col='dot_countryid', order_by_col1='createdon', order_by_col2='modifiedon')
df_crm_system_user = make_sql_table_unique('crm_system_user', unique_col='systemuserid', order_by_col1='createdon', order_by_col2='modifiedon')


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Contacts as found within accounts table

# COMMAND ----------

# DBTITLE 1,melt_ps_dataframe
df_acc_con_pivot = melt_ps_dataframe(df_crm_account, id_vars=['accountid'], value_vars= list(ACCOUNT_CONTACT_COLS.keys()), var_name='contact_type', value_name='contactid')
df_acc_con_pivot = df_acc_con_pivot.filter(F.col('contactid').isNotNull())

contact_mapping_expr = F.create_map([F.lit(x) for x in itertools.chain(*ACCOUNT_CONTACT_COLS.items())])
df_acc_con_pivot = df_acc_con_pivot.withColumn("contact_type_short", contact_mapping_expr[F.col("contact_type")])

# print(f"Rows before filter: {df_acc_con_pivot.count()}")
# Remove contacts not matching the pattern- accounts have been previously checked for pattern mismatches
df_acc_con_pivot = df_acc_con_pivot.filter(F.col('contactid').rlike(GUID_REGEX_PATTERN))
# print(f"Rows after filter: {df_acc_con_pivot.count()}")

df_acc_con_pivot = df_acc_con_pivot.groupBy('accountid', 'contactid').agg(F.collect_set('contact_type_short').alias('Contact_Type'))
# print(f"Rows after grouping: {df_acc_con_pivot.count()}")
df_acc_con_pivot = df_acc_con_pivot.withColumn('Source_Flag', F.lit('account_with_contact'))


# COMMAND ----------

# DBTITLE 1,df_crm_contacts_parent_acc
# Note: Very few rows (~400 of ~4M) are filled for parent_contactid so we are not additionally allocating contacts basis the account affiliation of their parent_contactid
# Contacts table
df_crm_contacts_parent_acc = df_crm_contacts.\
              filter(F.col('parentcustomerid').isNotNull()).\
              select('contactid', 'parentcustomerid').\
              withColumnRenamed('parentcustomerid', 'accountid')


# Remove any parentcustomerid (renamed as account now) not matching the pattern (contact pattern has already been checked before)
# print(f"Rows before: {df_crm_contacts_parent_acc.count()}")
df_crm_contacts_parent_acc = df_crm_contacts_parent_acc.filter(F.col('accountid').rlike(GUID_REGEX_PATTERN))
# print(f"Rows before: {df_crm_contacts_parent_acc.count()}")

# Filtering for valid accounts (i.e. if they are in the accounts table as well)
df_crm_contacts_parent_acc = df_crm_contacts_parent_acc.join(df_crm_account.select('accountid'), on='accountid', how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Merging contacts from accounts and contacts table/ Adding back all remaining accounts (with no contacts)

# COMMAND ----------

# DBTITLE 1,Merging contacts
# Contacts already under same account in accounts table are not appended again as we do an outer join
df_all_acc_contacts = df_acc_con_pivot.join(df_crm_contacts_parent_acc, on=['accountid', 'contactid'], how='outer')
# Whatever extra is appended should be coming from contacts table
df_all_acc_contacts = df_all_acc_contacts.withColumn('Source_Flag', F.when(F.col('Source_Flag').isNull(), 'contact_with_parentaccount').otherwise(F.col('Source_Flag')))

# Adding back accounts that have no contacts mapped and hence not part of above table
df_all_acc_contacts = df_all_acc_contacts.join(df_crm_account.select('accountid'), on='accountid', how='outer')
# Whatever extra is appended should be coming from accounts table
df_all_acc_contacts = df_all_acc_contacts.withColumn('Source_Flag', F.when(F.col('Source_Flag').isNull(), 'account_with_no_contact').otherwise(F.col('Source_Flag')))

df_all_acc_contacts.createOrReplaceTempView('all_acc_contacts')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Adding Account Manager information

# COMMAND ----------

# DBTITLE 1,Adding Account Manager information
df_accmgr_lookup_bu = df_crm_business_unit.select(F.col('businessunitid').alias('yc_businessunitid'), F.col('name').alias('businessunit_name'))
df_accmgr_lookup_status = df_crm_stringmap.\
                      filter((F.col('objecttypecode')=='yc_accountmanagerhistory') & (F.col('attributename')=='statuscode')).\
                      select(F.col('attributevalue').alias('statuscode'), F.col('value').alias('statuscode_name'))
df_accmgr_lookup_accmgr_name = df_crm_system_user.select(F.col('systemuserid').alias('yc_accountmanagerid'), F.col('fullname').alias('accountmanager_name'))

df_crm_account_manager_w_lookups = df_crm_account_manager.\
                                  join(df_accmgr_lookup_bu, on='yc_businessunitid', how='left').\
                                  join(df_accmgr_lookup_status, on='statuscode', how='left').\
                                  join(df_accmgr_lookup_accmgr_name, on='yc_accountmanagerid', how='left')

# COMMAND ----------

# # Checks for account manager table
# assert percInColumnForNull(df_crm_account_manager_w_lookups.filter("statuscode is not null"), 'statuscode_name') < 0.05, "Account manager statuscode lookup has >5% non-matches"
# assert percInColumnForNull(df_crm_account_manager_w_lookups.filter("yc_businessunitid is not null"), 'businessunit_name') < 0.05, "Account manager yc_businessunitid lookup has >5% non-matches"

# COMMAND ----------

# DBTITLE 1,account_manager lookups, history, pivot

df_crm_account_manager_w_lookups = df_crm_account_manager_w_lookups.\
                                  filter(F.col('businessunit_name').isNotNull()).\
                                  withColumn('accountmanager_name', F.when(F.col('accountmanager_name').isNull(),'Name_Not_Available').otherwise(F.col('accountmanager_name')))

windowSpec_accmgr_active = Window.partitionBy('yc_accountid', 'businessunit_name').orderBy(F.col("yc_startdate").desc(), F.col("createdon").desc())
df_crm_active_account_manager_pivot = df_crm_account_manager_w_lookups.\
             filter(F.lower('statuscode_name')=='active').\
             withColumn('businessunit_name', F.regexp_replace(F.lower('businessunit_name'), r'\s+', '_')).\
             withColumn('businessunit_name', F.concat(F.lit('Active_AccountMgr_'), F.col('businessunit_name'))).\
             withColumn('active_accountmanager', F.first('accountmanager_name').over(windowSpec_accmgr_active)).\
             groupBy('yc_accountid', 'businessunit_name').\
             agg(F.first('active_accountmanager').alias('active_accountmanager')).\
             groupBy('yc_accountid').\
             pivot('businessunit_name').\
             agg(F.first('active_accountmanager'))

df_account_manager_history = df_crm_account_manager_w_lookups.\
            filter(F.col('yc_accountmanagerid').isNotNull()).\
            withColumn('yc_startdate', F.to_date('yc_startdate')).\
            withColumn('yc_enddate', F.to_date('yc_enddate')).\
            groupBy('yc_accountid', 'businessunit_name', 'yc_accountmanagerid', 'accountmanager_name', 'statuscode_name', 'yc_startdate', 'yc_enddate').count().\
            groupBy('yc_accountid', 'businessunit_name', 'yc_accountmanagerid', 'accountmanager_name').agg(F.collect_list(F.struct('statuscode_name', 'yc_startdate','yc_enddate')).alias('dates')).\
            groupBy('yc_accountid', 'businessunit_name').agg(F.collect_list(F.create_map('accountmanager_name', 'dates')).alias('accmgr_info')).\
            groupBy('yc_accountid').agg(F.collect_list(F.create_map('businessunit_name', 'accmgr_info')).alias('AccountMgr_history'))


df_account_manager_history = df_account_manager_history.join(df_crm_active_account_manager_pivot, on='yc_accountid', how='left')
df_account_manager_history = df_account_manager_history.\
                select(['yc_accountid'] + [col for col in df_account_manager_history.columns if 'Active_AccountMgr_' in col] + ['AccountMgr_history'])

df_account_manager_history.createOrReplaceTempView('account_manager_history')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Adding Contract Information

# COMMAND ----------

df_cc_lookup_bu = df_crm_business_unit.select(F.col('businessunitid').alias('dot_businessunitid'), F.col('name').alias('businessunit_name'))
df_cc_lookup_status = df_crm_stringmap.\
                      filter((F.col('objecttypecode')=='dot_clientcontract') & (F.col('attributename')=='dot_contractstatus')).\
                      select(F.col('attributevalue').alias('dot_contractstatus'), F.col('value').alias('status_name'))

df_crm_client_contracts_w_lookups = df_crm_client_contracts.\
                                  join(df_cc_lookup_bu, on='dot_businessunitid', how='left').\
                                  join(df_cc_lookup_status, on='dot_contractstatus', how='left')


# COMMAND ----------

# Checks for Client Contract table
# assert percInColumnForNull(df_crm_client_contracts_w_lookups.filter("dot_contractstatus is not null"), 'status_name') < 0.05, "Client Contract dot_contractstatus lookup has >5% non-matches"
# assert percInColumnForNull(df_crm_client_contracts_w_lookups.filter("dot_businessunitid is not null"), 'businessunit_name') < 0.05, "Client Contract dot_businessunitid lookup has >5% non-matches"

# COMMAND ----------

# DBTITLE 1,Adding Contract Information
windowSpec_cc_active_v1 = Window.partitionBy('dot_accountid', 'flag_null_bu').orderBy(F.col("dot_startdate").asc(), F.col("createdon").asc())
windowSpec_cc_active_v2 = Window.partitionBy('dot_accountid', 'businessunit_name').orderBy(F.col("dot_startdate").desc(), F.col("createdon").desc())
df_crm_active_client_contracts_pivot = df_crm_client_contracts_w_lookups.\
                               filter(F.lower('status_name')=='active (signed)').\
                               withColumn('flag_null_bu', F.when(F.col('businessunit_name').isNull(), 1).otherwise(0)).\
                               withColumn('counter_null_bu', F.when(F.col('flag_null_bu')==1, F.row_number().over(windowSpec_cc_active_v1)).otherwise(None)).\
                               withColumn('str_null_bu', F.concat(F.lit('Missing_BU_'), F.col('counter_null_bu'))).\
                               withColumn('businessunit_name', F.when(F.col('flag_null_bu')==1, F.col('str_null_bu')).otherwise(F.col('businessunit_name'))).\
                               withColumn('businessunit_name', F.regexp_replace(F.lower('businessunit_name'), r'\s+', '_')).\
                               withColumn('businessunit_name', F.concat(F.lit('Active_Contract_'), F.col('businessunit_name'))).\
                               withColumn('active_contract', F.first('dot_contractnumber').over(windowSpec_cc_active_v2)).\
                               groupBy('dot_accountid', 'businessunit_name').\
                               agg(F.first('active_contract').alias('active_contract')).\
                               groupBy('dot_accountid').\
                               pivot('businessunit_name').\
                               agg(F.first('active_contract'))

df_client_contract_history = df_crm_client_contracts_w_lookups.\
            filter(F.col('dot_contractnumber').isNotNull()).\
            withColumn('businessunit_name', F.when(F.col('businessunit_name').isNull(), 'Missing_BU').otherwise(F.col('businessunit_name'))).\
            withColumn('dot_startdate', F.to_date('dot_startdate')).\
            withColumn('dot_enddate', F.to_date('dot_enddate')).\
            groupBy('dot_accountid', 'businessunit_name', 'dot_contractnumber', 'status_name', 'dot_startdate', 'dot_enddate').count().\
            groupBy('dot_accountid', 'businessunit_name', 'dot_contractnumber').agg(F.collect_list(F.struct('status_name', 'dot_startdate','dot_enddate')).alias('dates')).\
            groupBy('dot_accountid', 'businessunit_name').agg(F.collect_list(F.create_map('dot_contractnumber', 'dates')).alias('client_contract_info')).\
            groupBy('dot_accountid').agg(F.collect_list(F.create_map('businessunit_name', 'client_contract_info')).alias('ClientContract_history'))


df_client_contract_history = df_client_contract_history.join(df_crm_active_client_contracts_pivot, on='dot_accountid', how='left')
df_client_contract_history = df_client_contract_history.\
                select(['dot_accountid'] + [col for col in df_client_contract_history.columns if 'Active_Contract_' in col] + ['ClientContract_history'])

df_client_contract_history.createOrReplaceTempView('client_contract_history')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging all other information for contacts and accounts

# COMMAND ----------

# DBTITLE 1,Merging all other information for contacts and accounts
df_crm_acc_contact_rfp = spark.sql("""
WITH dot_preferredlanguage_smap AS (
  SELECT * FROM crm_stringmap 
  WHERE objecttypecode = 'contact' 
    AND attributename = 'dot_preferredlanguage'
)

SELECT 
    acc_con.accountid AS Account_ID,
    CASE 
        WHEN acc.emailaddress1 LIKE '%@%' THEN LOWER(acc.emailaddress1)
        WHEN acc.emailaddress2 LIKE '%@%' THEN LOWER(acc.emailaddress2)
        WHEN acc.emailaddress3 LIKE '%@%' THEN LOWER(acc.emailaddress3)
        ELSE NULL 
    END AS Account_Email,
    acc.name AS Account_Name,
    acc.description AS Account_Description,
    acc_segment.dot_name AS Account_Segment,
    acc_subsegment.dot_name AS Account_SubSegment,
    acc_country.dot_name AS Account_Country,
    CAST(acc.yc_availablecreditlimit AS STRING) AS Account_Credit_Limit,
    CAST(acc.dot_totalehcreditlimittravelbox AS STRING) AS Account_EH_TravelBox_Credit_Limit,
    accmgr_history.*,
    cc_history.*,
    acc_con.contactid AS Contact_ID,
    acc_con.Contact_Type,
    dot_preferredlanguage_smap.value AS Contact_Preferred_Language,
    con.mobilephone AS Contact_Mobile,
    CASE 
        WHEN con.telephone1 IS NOT NULL THEN con.telephone1
        WHEN acc.telephone2 IS NOT NULL THEN acc.telephone2
        WHEN acc.telephone3 IS NOT NULL THEN acc.telephone3
        ELSE NULL 
    END AS Contact_Telephone,
    CASE 
        WHEN con.emailaddress1 LIKE '%@%' THEN LOWER(con.emailaddress1)
        WHEN con.emailaddress2 LIKE '%@%' THEN LOWER(con.emailaddress2)
        ELSE NULL 
    END AS Contact_Email,
    con.fullname AS Contact_FullName,       
    acc_con.Source_Flag AS CRM_Source_Flag
FROM all_acc_contacts acc_con
LEFT JOIN crm_account acc ON acc_con.accountid = acc.accountid
LEFT JOIN crm_contacts con ON acc_con.contactid = con.contactid
LEFT JOIN account_manager_history accmgr_history 
    ON acc_con.accountid = accmgr_history.yc_accountid
LEFT JOIN client_contract_history cc_history 
    ON acc_con.accountid = cc_history.dot_accountid
LEFT JOIN crm_dot_segment acc_segment 
    ON acc.dot_segmentid = acc_segment.dot_segmentid
LEFT JOIN crm_dot_segment acc_subsegment 
    ON acc.dot_subsegmentid = acc_subsegment.dot_segmentid
LEFT JOIN crm_dot_country acc_country 
    ON acc.dot_country = acc_country.dot_countryid
LEFT JOIN dot_preferredlanguage_smap 
    ON con.dot_preferredlanguage = dot_preferredlanguage_smap.attributevalue
""")
df_crm_acc_contact_rfp.createOrReplaceTempView('crm_account_contacts')

# COMMAND ----------

# DBTITLE 1,Cell 35
# Validate table name to prevent SQL injection
import re
assert re.match(r'^[\w]+\.[\w]+\.[\w]+$', src_scv), f"Invalid table name format: {src_scv}"

df_crm_account_contacts_rfp = spark.sql(f"""
WITH scv_active AS (
    SELECT email_id, scv_account_sk
    FROM (
        SELECT 
            email_id, 
            scv_account_sk,
            ROW_NUMBER() OVER (
                PARTITION BY email_id
                ORDER BY parent_scv_account_sk ASC NULLS FIRST, modified_date DESC
            ) AS rn
        FROM {src_scv}
        where  source_name = 't_mdm_guest'
    ) tmp
    WHERE rn = 1
)

SELECT

    -- SCV mapping
    scv.scv_account_sk AS scv_account_sk,

   {email_standardization("Account_Email")} as account_email,
   {email_standardization("Contact_Email")} as contact_email,

    acc_con.Account_ID AS account_id,
    acc_con.Account_Name AS account_name,
    acc_con.Account_Description AS account_description,
    acc_con.Account_Segment AS account_segment,
    acc_con.Account_SubSegment AS account_subsegment,
    acc_con.Account_Country AS account_country,
    acc_con.Account_Credit_Limit AS account_credit_limit,
    acc_con.Account_EH_TravelBox_Credit_Limit AS account_eh_travelbox_credit_limit,
    acc_con.yc_accountid AS yc_accountid,

    acc_con.Active_AccountMgr_dmc AS active_accountmgr_dmc,
    acc_con.Active_AccountMgr_etihad_arena AS active_accountmgr_etihad_arena,
    acc_con.Active_AccountMgr_experience_hub AS active_accountmgr_experience_hub,
    acc_con.Active_AccountMgr_farah_experiences AS active_accountmgr_farah_experiences,
    acc_con.Active_AccountMgr_qasr_al_watan AS active_accountmgr_qasr_al_watan,
    acc_con.Active_AccountMgr_yas_concierge AS active_accountmgr_yas_concierge,
    acc_con.Active_AccountMgr_yascrm AS active_accountmgr_yascrm,

    acc_con.AccountMgr_history AS accountmgr_history,

    acc_con.dot_accountid AS dot_accountid,
    acc_con.Active_Contract_experience_hub AS active_contract_experience_hub,
    acc_con.Active_Contract_farah_experiences AS active_contract_farah_experiences,
    acc_con.Active_Contract_missing_bu_1 AS active_contract_missing_bu_1,
    acc_con.Active_Contract_missing_bu_2 AS active_contract_missing_bu_2,
    acc_con.Active_Contract_yascrm AS active_contract_yascrm,

    acc_con.ClientContract_history AS clientcontract_history,

    acc_con.Contact_ID AS contact_id,
    acc_con.Contact_Type AS contact_type,
    acc_con.Contact_Preferred_Language AS contact_preferred_language,
    acc_con.Contact_Mobile AS contact_mobile,
    acc_con.Contact_Telephone AS contact_telephone,
    acc_con.Contact_FullName AS contact_fullname,
    acc_con.CRM_Source_Flag AS crm_source_flag,

    'brz_crm' as source_name

FROM crm_account_contacts acc_con

LEFT JOIN scv_active scv
    ON LOWER(acc_con.Contact_Email) = LOWER(scv.email_id)
""")

# Add ETL timestamps via F.lit() instead of f-string injection
df_crm_account_contacts_rfp = df_crm_account_contacts_rfp \
    .withColumn('etl_created_datetime', F.lit(current_uae_timestamp).cast('timestamp')) \
    .withColumn('etl_updated_datetime', F.lit(current_uae_timestamp).cast('timestamp'))

df_crm_account_contacts_rfp = df_crm_account_contacts_rfp.toDF(
    *[c.lower() for c in df_crm_account_contacts_rfp.columns]
)

# COMMAND ----------

# DBTITLE 1,apply_schema_casting - ArrayType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, ArrayType

def apply_schema_casting(df, schema_dict):
    for column, dtype in schema_dict.items():
        if column in df.columns:
            if dtype.startswith("ARRAY"):
                df = df.withColumn(column, col(column).cast(ArrayType(StringType())))
            elif dtype.upper() == "STRING":
                df = df.withColumn(column, col(column).cast(StringType()))
            else:
                df = df.withColumn(column, col(column).cast(dtype))
    return df

# COMMAND ----------

# DBTITLE 1,apply_schema_casting
# Apply schema casting
df_crm_account_contacts_rfp = apply_schema_casting(df_crm_account_contacts_rfp, crm_acc_contact_rfp_schema)

# COMMAND ----------

# DBTITLE 1,Load
from pyspark.sql.functions import date_sub, current_date
from delta.tables import DeltaTable

# Build DDL string from schema dict
crm_acc_contact_rfp_ddl = ",\n".join(
    f"`{col}` {dtype}" for col, dtype in crm_acc_contact_rfp_schema.items()
)

try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(crm_acc_contact_rfp_ddl)

    # Initialize metrics
    inserted_count = 0
    updated_count  = 0

    if compare_schema_lenient(df_crm_account_contacts_rfp.schema, ddl_struct):
        print("Schema matches!")

        # ---- Schema regression check: detect removed columns before overwrite ----
        if spark.catalog.tableExists(target_table):
            existing_cols = set(spark.table(target_table).columns)
            incoming_cols = set(df_crm_account_contacts_rfp.columns)
            removed_cols = existing_cols - incoming_cols
            added_cols = incoming_cols - existing_cols

            if added_cols:
                print(f"INFO: New columns being added: {sorted(added_cols)}")

            if removed_cols:
                print(f"CRITICAL: Columns being removed: {sorted(removed_cols)}")
                print("This may break downstream consumers. Failing to prevent silent schema regression.")
                raise Exception(
                    f"Schema regression detected: columns {sorted(removed_cols)} exist in the "
                    f"current table but are missing from the incoming DataFrame. "
                    f"If this is intentional, drop the columns explicitly from the target table first."
                )
        else:
            print("INFO: Target table does not exist yet — creating fresh.")

        # -------------------- FULL LOAD --------------------
        # This notebook uses overwrite, so treat it as a full load.
        (
            df_crm_account_contacts_rfp.write
            .format("delta")
            .mode("overwrite")
            # columns addition/datatype modification in existing schema
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

        # Full load metrics: read count from the written table directly
        # (avoids re-executing the entire transformation DAG)
        inserted_count = spark.sql(
            f"SELECT COUNT(*) FROM {target_table}"
        ).collect()[0][0]
        updated_count  = 0

        print(f"df_crm_acc_contact_rfp Table New Data Loaded: {inserted_count}")

    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_crm_account_contacts_rfp.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")

    # -------------------- FINAL COUNT --------------------
    print(f"crm_acc_contact_rfp Table Loaded. Total Records: {inserted_count}")
    print(f"Rows Inserted : {inserted_count}")
    print(f"Rows Updated  : {updated_count}")

except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

#view creation
catalog     = f"transformation_{env}"
schema      = "plt_customer360"
object_name = "vw_crm_account_contacts_rfp"
select_query = c360_view_queries[object_name].format(catalog=catalog, schema=schema)
create_view_or_table(catalog=catalog, schema=schema, object_name=object_name, select_query=select_query)

# COMMAND ----------

# DBTITLE 1,add last_updated tag
# add last_updated tag with current timestamp to identify table last refreshed datetime.
add_last_updated_tag(target_table)

# COMMAND ----------

# DBTITLE 1,Cell 42
plt_stats = {
    "gld_table"                   : target_table,
    "dim_new_records_count"       : 0,
    "dim_updated_records_count"   : 0,
    "fact_inserted_records_count" : inserted_count,
    "fact_updated_records_count"  : updated_count
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)

# COMMAND ----------

trigger_databricks_job_by_name('workflow_orchestration')
