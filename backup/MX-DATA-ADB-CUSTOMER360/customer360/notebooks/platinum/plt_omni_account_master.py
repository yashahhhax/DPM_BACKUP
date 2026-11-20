# Databricks notebook source
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC **The `omni_account_master` table consolidates account data from the gold source (`gld_drr.dwh_lkp_vw_account`) into a platinum-level analytics table, enforcing schema and loaded via a full load strategy.**
# MAGIC 1. Source Gold Table:
# MAGIC | Schema | Table |
# MAGIC | --- | --- |
# MAGIC |gld_drr |dwh_lkp_vw_account |
# MAGIC
# MAGIC 2. Target Platinum table: **omni_account_master**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 
# MAGIC
# MAGIC 4. Load Strategy: **Full Load**
# MAGIC

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
# dbutils.widgets.text("env","dev")
# env = dbutils.widgets.get("env")
# dbutils.widgets.text("current_job_run_id", "")
# current_job_run_id = dbutils.widgets.get("current_job_run_id")

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include base fucntions
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,get notebook parameters
try:
    notebook_vars = [
        'env','current_job_run_id'
    ]
    env,current_job_run_id= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")


# COMMAND ----------

# DBTITLE 1,import  dependencies
from pyspark.sql.functions import col, when, current_timestamp, trim, lower, lit, regexp_replace,regexp_extract, instr, split, row_number, to_date, date_format, upper,min,max,expr,coalesce,length,year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime
from zoneinfo import ZoneInfo


# COMMAND ----------

# DBTITLE 1,initialize source tables
#source schema
drr_schema = f"transformation_{env}.gld_drr"
shared_schema = f"transformation_{env}.gld_shared"
sharepoint_schema = f"transformation_{env}.brz_sharepoint_file"
#source table
lkp_account_table = f"{drr_schema}.dwh_lkp_vw_account"
country_table = f"{shared_schema}.dwh_dim_country"
city_mapping = f"{sharepoint_schema}.vw_customer360_city_mapping"


# Target table name
target_table = f"transformation_{env}.plt_customer360.omni_account_master"
current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table : {lkp_account_table}")
print(f"Target table name: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,omni master  transformation
spark.sql(f"""
            ----------scan omni account table only once------------------
            with omni_account as (
                select * from {lkp_account_table}
            ),staff_flag as(
            --------------------Staff Flag-------------------- 
            select
            accountid
            from  omni_account
            where 1=1
            and (
                    (
                        (
                        lower(emailaddress1) in (
                                select
                                distinct lower(emailaddress1)
                                from omni_account
                                where 
                                dmgcategoryid in (121,182,40)
                                and emailaddress1 is not null
                            )
                        or 
                        split_part(lower(emailaddress1),'@',2) in ('miral.ae','farahexperiences.com','wbworld.com','ferrariworld-ad.com','experiencehub.com','yaswaterworld.com','seaworldabudhabi.com','qasralwatan.ae','clymbabudhabi.com','yasconcierge.com')
                        or 
                        split_part(lower(emailaddress2),'@',2) in ('miral.ae','farahexperiences.com','wbworld.com','ferrariworld-ad.com','experiencehub.com','yaswaterworld.com','seaworldabudhabi.com','qasralwatan.ae','clymbabudhabi.com','yasconcierge.com')
                        )
                    and parentaccountid is null
                    )
                or 
                    dmgcategoryid in (121,182,40)
            )
            ),dependent_flag as (
            -------------------- Dependent Flag -------------------- 
            select
            accountid
            from omni_account
            where 1=1
            and (
                    dmgcategoryid in (123,185,201)
                or
                (
                    lower(emailaddress1) in (
                        select
                        distinct lower(emailaddress1)
                        from omni_account
                        where 
                        dmgcategoryid in (121,182,40)
                        and emailaddress1 is not null
                    )
                    and parentaccountid is not null
                )
            )
            ), pod_flag as(
            --------------------  POD Flag -------------------- 
            select
            accountid
            from omni_account
            where 1=1
            and dmgcategoryid = 200
            )

            -------------------- Main Master query -------------------- 
            select
                {hash_column("ac.accountid")} as omni_account_sk
                ,{hash_column("ac.parentaccountid")} as parent_omni_account_sk
                ,ac.accountid as account_id
                ,firstname as first_name
                ,surname as last_name
                ,CASE 
                    WHEN length(regexp_replace(country, '[^A-Za-z]', '')) = 2 
                        THEN regexp_replace(country, '[^A-Za-z]', '')
                    ELSE NULL 
                 END as country_code
                ,CASE 
                    WHEN length(regexp_replace(custom1, '[^A-Za-z]', '')) = 2 
                        THEN regexp_replace(custom1, '[^A-Za-z]', '')
                    ELSE NULL 
                 END as nationality_code
                ,parentaccountid  as parent_account_id
                ,mobilephone
                ,gender
                ,birthdate
                ,(IFF( emailaddress1 like '%@%@%', split_part(emailaddress1,',',1) ,emailaddress1)) as email_id
                ,(coalesce( IFF( emailaddress1 like '%@%@%', split_part(emailaddress1,',',2) ,NULL) ,emailaddress2)) as alternate_email_id
                ,insdatetime as created_date
                ,upddatetime as updated_date              
                ,ac.city
                ---------------------------All Flags------------------------------
                ,CASE WHEN sf.accountid is not null THEN 'Y' ELSE 'N' END as staff_flag
                ,CASE WHEN df.accountid is not null THEN 'Y' ELSE 'N' END as dependent_flag
                ,CASE WHEN pf.accountid is not null THEN 'Y' ELSE 'N' END as pod_flag          
                
                ,IFF( staff_flag = 'Y' and try_cast(trim(ac.externalcode1) as int) is not null,  try_cast(trim(ac.externalcode1) as int)   ,NULL) as employee_id

            from omni_account ac
            left join staff_flag sf on sf.accountid = ac.accountid
            left join dependent_flag df on df.accountid = ac.accountid
            left join pod_flag pf on pf.accountid = ac.accountid
            qualify( row_number() over (partition by ac.accountid order by upddatetime desc)) = 1

""").createOrReplaceTempView("omni_master_raw")


# COMMAND ----------

# DBTITLE 1,Transformation and cleaning
df_omni_account_master = spark.sql(f"""
            with parent_emails as (
                select account_id, email_id
                from omni_master_raw
            )
            select 
                ac.omni_account_sk
                ,ac.parent_omni_account_sk
                ,ac.account_id
                ,initcap(lower(ac.first_name)) as first_name
                ,initcap(lower(ac.last_name)) as last_name
                ,ac.parent_account_id
                ,ac.created_date
                ,ac.updated_date
                ,ac.staff_flag
                ,ac.dependent_flag
                ,ac.pod_flag
                ---------------------------city mapping------------------------------
                ,ac.city
                ,CASE WHEN COALESCE(city.city, ac.city) in ('NULL','n/a') THEN NULL 
                      WHEN COALESCE(city.city, ac.city) not rlike '[A-Za-z]' THEN NULL
                    ELSE COALESCE(city.city, ac.city) END AS city_cleaned
                ---------------------------country mapping---------------------------
                ,ac.country_code
                ,con.country_name as country_name
                ---------------------------Nationality mapping---------------------------
                ,ac.nationality_code
                ,nat.nationality_name as nationality_name
                ---------------------------Email id cleaning---------------------------
                ,{email_standardization("coalesce(ac.email_id,parent_account.email_id)")} as email_id
                ,coalesce(ac.email_id,parent_account.email_id) as raw_email_id
                ,{email_standardization("ac.alternate_email_id")} as alternate_email_id
                ,ac.alternate_email_id as raw_alternate_email_id

                ,ac.mobilephone as raw_mobile_num
                ,ac.gender
                ,ac.birthdate
                ,ac.employee_id
                ,'dwh_lkp_vw_account' as SOURCE
            from omni_master_raw ac
            left join {country_table} con on con.country_code = ac.country_code and con.CURRENT_FLAG = 'Y'
            left join {country_table} nat on nat.country_code = ac.nationality_code and nat.CURRENT_FLAG = 'Y'
            left join {city_mapping} city on lower(trim(replace(city.city_value,' ',''))) = lower(trim(replace(ac.city,' ','')))
            left join parent_emails parent_account on parent_account.account_id = ac.parent_account_id
          """) 

# COMMAND ----------

# DBTITLE 1,DQC
#---------------------------Applying Data Quality Standardization------------------------------
#mobile number standardization
df_omni_account_master = master_mobile_number_standardization(df_omni_account_master,"raw_mobile_num")

#gender standardization
df_omni_account_master = gender_standardization(df_omni_account_master,"gender")

#dob standardization
df_omni_account_master = dob_standardization(df_omni_account_master, "birthdate")\
.withColumn("dob", when((year(col("dob")) >= 1800) & (year(col("dob")) <= datetime.now().year), col("dob")).otherwise(lit(None)))

df_omni_account_master.createOrReplaceTempView("omni_master")  

# COMMAND ----------

# DBTITLE 1,column ordering
df_omni_account_master = spark.sql(f"""
    select
        --//primary key
        omni_account_sk

        --// Infographics
        ,account_id
        ,email_id
        ,alternate_email_id
        ,first_name
        ,last_name
        ,dob
        ,gender_cleaned as gender
        ,mobile_country_code
        ,mobile_num_cleaned as mobile_num

        --// Demographics
        ,city_cleaned as city
        ,nationality_code
        ,nationality_name
        ,country_code as country_of_residence_code
        ,country_name as country_of_residence_name

        --// Flags
        ,staff_flag
        ,dependent_flag
        ,pod_flag

        --//parent details
        ,parent_omni_account_sk
        ,parent_account_id

        --//ETL timestamp
        ,created_date
        ,updated_date
        ,SOURCE as source_name
        ,'{current_uae_timestamp}' as etl_created_datetime
        ,'{current_uae_timestamp}' as etl_updated_datetime

        --//Raw data
        ,map(
            'email_id', raw_email_id,
            'alternate_email_id', raw_alternate_email_id,
            'mobile_num', raw_mobile_num,
            'city', city,
            'dob', birthdate,
            'gender', gender,
            'employee_id', cast(employee_id as string)
        ) as raw_data_json
    from omni_master
""")

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_omni_account_master = apply_schema_casting(df_omni_account_master, omni_account_master_schema)


# COMMAND ----------

# DBTITLE 1,write to UC
# Build DDL string from schema dict
omni_master_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in omni_account_master_schema.items()
)
 
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(omni_master_ddl)
    if compare_schema_lenient(df_omni_account_master.schema, ddl_struct):
        print("Schema matches!")
        (df_omni_account_master.write.format("delta")
                        .mode("overwrite")
                        .option("mergeSchema", "true")
                        .saveAsTable(target_table))
        print(f"Omni Master Table Loaded...")
        row_count = spark.table(target_table).count()
        print(f"Total Records: {row_count}")
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_omni_account_master.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# add last_updated tag with current timestamp to identify table last refreshed datetime.
add_last_updated_tag(target_table)

# COMMAND ----------

# Prepare platinum table statistics dictionary for logging

plt_stats = {
    "gld_table": target_table,
    "dim_new_records_count": row_count,
    "dim_updated_records_count": 0,
    "fact_inserted_records_count": 0,
    "fact_updated_records_count": 0
}

log_gld_table_stats(
    usecase="customer360",
    current_job_run_id=current_job_run_id,
    table_name=target_table,
    gold_stats=plt_stats
)

# COMMAND ----------

# DBTITLE 1,trigger workflow
trigger_databricks_job_by_name('workflow_orchestration')
