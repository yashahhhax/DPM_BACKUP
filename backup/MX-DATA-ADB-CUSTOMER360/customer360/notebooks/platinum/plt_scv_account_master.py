# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %md
# MAGIC ## SUMMARY
# MAGIC **The `scv_account_master` table serves as the consolidated platinum-level account master, integrating data from multiple SCV gold tables. It enforces schema consistency and is loaded via a full load strategy, providing a unified source for advanced analytics and reporting.**
# MAGIC
# MAGIC 1. Source Gold Table: 
# MAGIC  Schema | Table |
# MAGIC  --- | --- |
# MAGIC brz_scv | vw_dbo_t_mdm_guest |
# MAGIC gld_drr | tbvw_attendance |
# MAGIC brz_scv | vw_dbo_t_mdm_rel |
# MAGIC gld_facepassreporting | gld_lkp_guest_relationships |
# MAGIC slv_facepassreporting | slv_lkp_frictionless_data_account_snapshot |
# MAGIC gld_facepassreporting | gld_lkp_portrait_info |
# MAGIC
# MAGIC
# MAGIC 2. Target Platinum table: **scv_account_master**
# MAGIC
# MAGIC 3. Schema Enforcement implemented 
# MAGIC
# MAGIC 4. Load Strategy: **Full Load**

# COMMAND ----------

# DBTITLE 1,import library
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, col,add_months, current_date,expr,year
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,declare and retrive parameters
dbutils.widgets.text("env","dev")
env = dbutils.widgets.get("env")
dbutils.widgets.text("current_job_run_id", "")
current_job_run_id = dbutils.widgets.get("current_job_run_id")

# COMMAND ----------

# DBTITLE 1,include schema config
# MAGIC %run ../../config/schema_config

# COMMAND ----------

# DBTITLE 1,include base functions
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

# DBTITLE 1,initialize table names
facepass_schema = f"transformation_{env}.gld_facepassreporting"

#t_mdm_guest
t_mdm_guest = f"transformation_{env}.brz_scv.vw_dbo_t_mdm_guest"
#t_mdm_rel
t_mdm_rel   = f"transformation_{env}.brz_scv.vw_dbo_t_mdm_rel"
#friction less
guest_relationships = f"{facepass_schema}.gld_lkp_guest_relationships"
frictionless_data_accounts = f"transformation_{env}.slv_facepassreporting.slv_lkp_frictionless_data_account_snapshot"
portrait_info = f"{facepass_schema}.gld_lkp_portrait_info"
#attendance
attendance_table = f"transformation_{env}.gld_drr.tbvw_attendance"

city_mapping = f"transformation_{env}.brz_sharepoint_file.vw_customer360_city_mapping"
country_table = f"transformation_{env}.gld_shared.dwh_dim_country"

# Target table name
target_table = f"transformation_{env}.plt_customer360.scv_account_master"

current_uae_timestamp = datetime.now(ZoneInfo("Asia/Dubai")).strftime("%Y-%m-%d %H:%M:%S")

print(f"Source table: {t_mdm_guest}")
print(f"Source table: {t_mdm_rel}")
print(f"Source table: {guest_relationships}")
print(f"Source table: {frictionless_data_accounts}")
print(f"Source table: {portrait_info}")
print(f"Source table: {attendance_table}")
print(f"Target table: {target_table}")
print(f"Current timestamp: {current_uae_timestamp}")

# COMMAND ----------

# DBTITLE 1,frictionless core
df_scv_account_master = spark.sql(f"""
--Add CTE for dedup data accounts table (based on partiion by mypass id, order by snapdate desc)
with frictionless_data_accounts as (
    select 
    mypassid,
    accountid
    from {frictionless_data_accounts} 
    qualify row_number() over (partition by mypassid order by snapdate desc,updated desc) = 1
)
,facetag_accounts as (
select
    portrait_info.out_biz_id as mypassid
    ,data_accounts.accountid as fl_account_id
    ,initcap(ifnull(replace(parse_json(portrait_info.meta_info):`firstName`,'"',''),name)) as first_name
    ,initcap(replace(parse_json(portrait_info.meta_info):`lastName`,'"','')) as last_name
    ,parse_json(portrait_info.meta_info):`ticketReferenceNumber`::string as media_identifier
    ,portrait_info.create_time as create_time
    ,portrait_info.update_time as update_time
from {portrait_info} portrait_info
left join frictionless_data_accounts data_accounts  --Use deduped CTE
    on data_accounts.MYPASSID = portrait_info.out_biz_id
where portrait_info.out_biz_id not like '%-%'
    and length(portrait_info.out_biz_id) > 25
    and parse_json(portrait_info.meta_info):`ticketReferenceNumber` is not null
    and initcap(ifnull(replace(parse_json(portrait_info.meta_info):`firstName`,'"',''),name)) != 'Facecapture'
qualify ( row_number() over (partition by portrait_info.out_biz_id  order by portrait_info.UPDATE_TIME desc)) = 1
)

select
    IFF(nullif(tbvw_attendance.`AP ACCOUNT ID`,'79269') > 0  AND tbvw_attendance.`AP EMAIL ADDRESS` like '%@%' , tbvw_attendance.`AP ACCOUNT ID`, NULL) as omni_account_id
    ,IFF(nullif(tbvw_attendance.`AP ACCOUNT ID`,'79269') > 0  AND tbvw_attendance.`AP EMAIL ADDRESS` like '%@%' , tbvw_attendance.`AP EMAIL ADDRESS`, NULL) as omni_email_address
    ,ft_acc.fl_account_id as fl_account_id
    ,ft_acc.mypassid as fl_mypassid
    ,ft_acc.first_name as fl_first_name
    ,ft_acc.last_name as fl_last_name
    ,ft_acc.media_identifier as mediaidentifier
    ,ft_acc.create_time
    ,ft_acc.update_time
from facetag_accounts ft_acc
left join {attendance_table} tbvw_attendance
on ft_acc.media_identifier = tbvw_attendance.`Media Code`
where 1=1
qualify ( row_number() over (partition by ft_acc.mypassid  order by ifnull(tbvw_attendance.`Attendance Date`,ft_acc.update_time)  desc)) = 1
-- order by 1,2,3,4,5
""")
df_scv_account_master.createOrReplaceTempView("base_ft_accounts")

# COMMAND ----------

# DBTITLE 1,business logic and flags
df_scv_account_master = spark.sql(f"""
WITH t_mdm_guest (
    select
    yas_id as raw_yas_id 
    ,email as raw_email
    ,alternate_email as raw_alternate_email
    --cleaning yas_id and email id
    ,{email_standardization("email")} AS email
    ,{yas_id_standardization("yas_id")} as yas_id
    ,{email_standardization("alternate_email")}as alternate_email
    ,IFF(raw_yas_id is not null,1,0) as yas_id_exists_flag
    ,* except(yas_id,email,alternate_email)
    from {t_mdm_guest}
), t_mdm_rel (
    SELECT
    YASID as raw_yas_id 
    ,parentemail as raw_parentemail
    ,from_yas_id as raw_from_yas_id
    --cleaning yas_id and from_yas_id and parent_email
    ,{email_standardization("parentemail")} AS parentemail
    ,{yas_id_standardization("YASID")} as yasid
    ,{yas_id_standardization("from_yas_id")} as from_yas_id
    ,* except(YASID,parentemail,from_yas_id)
    FROM {t_mdm_rel}
)
-- Latest parent records (deduped)
,scv_parent AS (
    SELECT
        mdm_id
        ,yas_id
        ,raw_yas_id 
        ,email
        ,raw_email
        ,alternate_email
        ,raw_alternate_email
        ,first_name
        ,last_name
        ,mobile_num
        ,gender
        ,dob
        ,country
        ,city
        ,nationality
        ,COALESCE(element_at(map('en','English','ar','Arabic','fr','French','de','German','es','Spanish','hi','Hindi','ur','Urdu','zh','Chinese','ja','Japanese','ko','Korean','ru','Russian','it','Italian','pt','Portuguese','tr','Turkish'), lower(trim(language))), initcap(language)) AS language
        ,guest_type
        ,created_date
        ,modified_date
        --to eliminate duplicate records while joining with child table
        ,IFF( 
            row_number() over (partition by yas_id order by modified_date desc) = 1
            , 'Y','N') 
        as active_yas_id_flag
        
    FROM t_mdm_guest
    WHERE 1=1
    and ifnull(delete_flg,false) = false -- if delete_flg is null considered as false
    QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY yas_id_exists_flag desc, modified_date DESC) = 1
),

-- Latest child records (deduped)
scv_child AS (
    SELECT
        NULL AS mdm_id
        ,from_yas_id
        ,raw_from_yas_id
        ,yasid AS yas_id
        ,raw_yas_id 
        ,parentemail AS email
        ,raw_parentemail
        ,first_name 
        ,last_name
        ,CASE 
            WHEN role_to = 'الأب' THEN 'Father'
            WHEN role_to = 'الأخ' THEN 'Brother'
            WHEN role_to = 'الأخت' THEN 'Sister'
            WHEN role_to = 'الأم' THEN 'Mother'
            WHEN role_to = 'الابن' THEN 'Son'
            WHEN role_to = 'الابنة' THEN 'Daughter'
            WHEN role_to = 'الزوج' THEN 'Husband'
            WHEN role_to = 'الزوجة' THEN 'Wife'
            WHEN role_to = 'الطفل' THEN 'Child'
            WHEN role_to = 'تابع' THEN 'Dependent'
            ELSE nullif(role_to,'')
         END AS role_to_parent
        ,NULL AS mobile_num 
        ,CASE
            WHEN role_to_parent IN ('Father', 'Brother', 'Husband', 'Son') THEN 'M'
            WHEN role_to_parent IN ('Mother', 'Sister', 'Wife', 'Daughter') THEN 'F'
            WHEN role_to_parent IN ('Child', 'Dependent', 'Spouse') THEN NULL
            ELSE NULL
         END AS gender
        ,dob
        ,NULL AS country
        ,NULL AS city 
        ,NULL AS nationality
        ,NULL AS language
        ,NULL AS guest_type 
        ,created_date 
        ,modified_date
    FROM t_mdm_rel
    QUALIFY ROW_NUMBER() OVER (PARTITION BY yasid ORDER BY modified_date DESC) = 1
),

-- Staff
scv_staff_emails AS (
    SELECT DISTINCT email AS staff_email
    FROM t_mdm_guest
    WHERE TRIM(LOWER(SPLIT_PART(IFNULL(alternate_email, email), '@', 2))) IN (
        'yasconcierge.com','wbworld.com','seaworldabudhabi.com','qasralwatan.ae','miraxl.ae',
        'miral123.ae','miral.xyz','miral.ae.xyz','miral.ae','masarra.ae','ferrariworld-ad.com',
        'farahexperiences.com','experiencehub.com','etihadarena.ae' -- add config instead of static values
    )
    OR yas_id IN (
        SELECT mypass_id FROM {guest_relationships}
        WHERE NULLIF(employement, '') IN ('Farah:Y', 'Miral:Y')
    )
),

-- Dependents
scv_dependent_yasids AS (
    SELECT DISTINCT yas_id as yasid
    FROM scv_child
    WHERE from_yas_id IN (
        SELECT DISTINCT yas_id
        FROM t_mdm_guest
        WHERE TRIM(LOWER(SPLIT_PART(IFNULL(alternate_email, email), '@', 2))) IN (
        'yasconcierge.com','wbworld.com','seaworldabudhabi.com','qasralwatan.ae','miraxl.ae',
        'miral123.ae','miral.xyz','miral.ae.xyz','miral.ae','masarra.ae','ferrariworld-ad.com',
        'farahexperiences.com','experiencehub.com','etihadarena.ae'
        )
    OR yas_id IN (
        SELECT DISTINCT mypass_id FROM {guest_relationships}
        WHERE NULLIF(employement, '') IN ('Farah:Y', 'Miral:Y')
    ))
    and role_to_parent in ('Child','Daughter','Dependent','Husband','Son','Spouse','Wife')
),

-- POD
scv_pod_emails AS (
    SELECT DISTINCT email AS pod_email
    FROM t_mdm_guest
    WHERE person_of_determination = TRUE
),

-- Facepass
facepass_yasids AS (
    SELECT DISTINCT mypassid AS yas_id
    FROM {frictionless_data_accounts}
    WHERE faceenrolled = 1
)

-- Parent records
SELECT
    {hash_column("p.email")} AS scv_account_sk
    ,NULL AS parent_scv_account_sk
    ,p.mdm_id
    ,p.yas_id
    ,p.raw_yas_id
    ,p.email as email
    ,p.raw_email
    ,p.alternate_email
    ,p.raw_alternate_email
    ,p.first_name 
    ,p.last_name 
    ,p.mobile_num 
    ,p.gender 
    ,p.dob
    ,p.country 
    ,p.city 
    ,p.nationality
    ,p.language
    ,p.guest_type
    ,CASE WHEN s.staff_email IS NOT NULL THEN 'Y' ELSE 'N' END AS staff_flag
    ,CASE WHEN pod.pod_email IS NOT NULL THEN 'Y' ELSE 'N' END AS pod_flag
    ,'N' AS dependent_flag
    ,CASE WHEN f.yas_id IS NOT NULL THEN 'Y' ELSE 'N' END AS facepass_enrolled_flag
    ,NULL AS parent_yas_id
    ,NULL as raw_parent_yas_id
    ,p.created_date
    ,p.modified_date
    ,p.active_yas_id_flag
    ,NULL as role_to_parent
    ,NULL as ft_account_id
    ,NULL as ft_mediaidentifier
    ,'t_mdm_guest' as SOURCE

FROM scv_parent p
LEFT JOIN scv_staff_emails s ON p.email = s.staff_email
LEFT JOIN scv_pod_emails pod ON p.email = pod.pod_email
LEFT JOIN facepass_yasids f ON p.yas_id = f.yas_id
WHERE p.email IS NOT NULL

UNION ALL

-- Child records
SELECT
    {hash_column("c.yas_id")} AS scv_account_sk
    ,{hash_column("p.email")} AS parent_scv_account_sk
    ,NULL AS mdm_id
    ,c.yas_id
    ,c.raw_yas_id
    ,c.email as email
    ,c.raw_parentemail as raw_email
    ,NULL as alternate_email
    ,NULL as raw_alternate_email
    ,c.first_name 
    ,c.last_name 
    ,c.mobile_num 
    ,c.gender
    ,c.dob 
    ,c.country 
    ,c.city 
    ,c.nationality
    ,p.language
    ,c.guest_type
    ,'N' AS staff_flag
    ,'N' AS pod_flag
    ,CASE WHEN d.yasid IS NOT NULL THEN 'Y' ELSE 'N' END AS dependent_flag
    ,CASE WHEN f.yas_id IS NOT NULL THEN 'Y' ELSE 'N' END AS facepass_enrolled_flag
    ,c.from_yas_id AS parent_yas_id
    ,c.raw_from_yas_id as raw_parent_yas_id
    ,c.created_date
    ,c.modified_date
    ,'Y' as active_yas_id_flag
    ,c.role_to_parent as role_to_parent
    ,NULL as ft_account_id
    ,NULL as ft_mediaidentifier
    ,'t_mdm_rel' as SOURCE

FROM scv_child c
LEFT JOIN scv_parent p ON p.yas_id = c.from_yas_id and p.active_yas_id_flag = 'Y'
LEFT JOIN scv_dependent_yasids d ON c.yas_id = d.yasid
LEFT JOIN facepass_yasids f ON c.yas_id = f.yas_id
WHERE c.yas_id is not null

UNION ALL

SELECT
    {hash_column("ft.fl_mypassid")} AS scv_account_sk
    ,{hash_column("p.email")} AS parent_scv_account_sk
    ,NULL AS mdm_id
    ,ft.fl_mypassid as yas_id
    ,ft.fl_mypassid as raw_yas_id
    ,{email_standardization("ft.omni_email_address")} as email
    ,ft.omni_email_address as raw_email
    ,NULL as alternate_email
    ,NULL as raw_alternate_email
    ,ft.fl_first_name as first_name 
    ,ft.fl_last_name as last_name 
    ,NULL AS mobile_num 
    ,NULL AS gender
    ,NULL AS dob 
    ,NULL AS country 
    ,NULL AS city 
    ,NULL AS nationality
    ,NULL AS language 
    ,NULL AS guest_type
    ,'N' AS staff_flag
    ,'N' AS pod_flag
    ,'N' AS dependent_flag
    ,'Y' AS facepass_enrolled_flag
    ,p.yas_id AS parent_yas_id
    ,p.raw_yas_id as raw_parent_yas_id
    ,ft.create_time as created_date
    ,ft.update_time as modified_date
    ,'Y' as active_yas_id_flag
    ,NULL as role_to_parent
    ,ft.fl_account_id as ft_account_id
    ,ft.mediaidentifier as ft_mediaidentifier
    ,'frictionless_face_tag' as SOURCE

FROM base_ft_accounts ft
LEFT JOIN scv_parent p ON p.email = {email_standardization("ft.omni_email_address")}
WHERE ft.fl_mypassid is not null

;
""")
df_scv_account_master.createOrReplaceTempView("base_scv_account_master")

# COMMAND ----------

# DBTITLE 1,transformation
df_scv_account_master = spark.sql(f"""
    SELECT 
        scv_account_sk
        ,parent_scv_account_sk
        ,mdm_id
        ,yas_id
        ,raw_yas_id
        ,parent_yas_id
        ,raw_parent_yas_id
        ,email as email_id
        ,raw_email as raw_email_id
        ,alternate_email as alternate_email_id
        ,raw_alternate_email as raw_alternate_email_id
        ,initcap(first_name) as first_name
        ,initcap(last_name) as last_name
        ,language
        ,guest_type
        ,staff_flag
        ,pod_flag
        ,dependent_flag
        ,facepass_enrolled_flag
        ,active_yas_id_flag
        ,created_date
        ,modified_date
        --------------------------city mapping--------------------------
        ,scv.city
        ,CASE WHEN COALESCE(cm.city, scv.city) = 'NULL' or COALESCE(cm.city, scv.city) = '' THEN NULL 
                    ELSE COALESCE(cm.city, scv.city) END AS city_cleaned
        ---------------------------country mapping---------------------------
        ,CASE WHEN country = 'Not Available' THEN NULL ELSE country END as country_name
        ,con.country_code as country_code
        ---------------------------nationality mapping---------------------------
        ,CASE WHEN nationality = 'Not Available' THEN NULL ELSE nationality END as nationality_name
        ,nat.country_code as  nationality_code 
        
        ,mobile_num as raw_mobile_num
        ,dob
        ,gender
        ,role_to_parent
        ,ft_account_id
        ,ft_mediaidentifier
        ,SOURCE
FROM base_scv_account_master scv
LEFT JOIN {country_table} con on lower(trim(con.COUNTRY_NAME)) = lower(trim(scv.country)) and con.CURRENT_FLAG = 'Y'
LEFT JOIN {country_table} nat on lower(trim(nat.COUNTRY_NAME)) = lower(trim(scv.nationality)) and nat.CURRENT_FLAG = 'Y'
LEFT JOIN {city_mapping} cm ON lower(trim(replace(cm.city_value,' ',''))) = lower(trim(replace(scv.city,' ','')))
""")


# COMMAND ----------

# DBTITLE 1,DQC
#---------------------------Applying Data Quality Standardization------------------------------
#mobile number standardization
df_scv_account_master = master_mobile_number_standardization(df_scv_account_master,"raw_mobile_num")

#gender standardization
df_scv_account_master = gender_standardization(df_scv_account_master,"gender")

#dob standardization
df_scv_account_master = df_scv_account_master.withColumnRenamed("dob","raw_dob")\
            .withColumn("dob", when((year(col("raw_dob")) >= 1800) & (year(col("raw_dob")) <= datetime.now().year), col("raw_dob")).otherwise(lit(None)))

df_scv_account_master.createOrReplaceTempView("scv_account_master")

# COMMAND ----------

# DBTITLE 1,column reordering
df_scv_account_master = spark.sql(f"""
    select
        --//primary key
        scv_account_sk

        --// Infographics
        ,mdm_id
        ,yas_id
        ,email_id
        ,alternate_email_id
        ,first_name
        ,last_name
        ,dob
        ,gender_cleaned as gender
        ,language
        ,role_to_parent
        ,guest_type
        ,mobile_country_code
        ,mobile_num_cleaned as mobile_num

        --// Demographics
        ,city_cleaned as city
        ,country_code as country_of_residence_code
        ,country_name as country_of_residence_name
        ,nationality_code
        ,nationality_name

        --// Flags
        ,staff_flag
        ,pod_flag
        ,dependent_flag
        ,facepass_enrolled_flag
        ,active_yas_id_flag

        --//parent details
        ,parent_scv_account_sk
        ,parent_yas_id

        --//ETL timestamp
        ,created_date
        ,modified_date
        ,SOURCE as source_name
        ,'{current_uae_timestamp}' as etl_created_datetime
        ,'{current_uae_timestamp}' as etl_updated_datetime

        --//Raw data
        ,map(
            'yas_id', raw_yas_id,
            'parent_yas_id', raw_parent_yas_id,
            'email_id', raw_email_id,
            'alternate_email_id', raw_alternate_email_id,
            'city', city,
            'mobile_num', raw_mobile_num,
            'dob', string(raw_dob),
            'gender', gender,
            'facetag_frictionless_account_id',string(ft_account_id),
            'facetag_media_identifier',string(ft_mediaidentifier)
        ) as raw_data_json
    from scv_account_master
""")

# COMMAND ----------

# DBTITLE 1,schema casting
# Apply schema casting
df_scv_account_master = apply_schema_casting(df_scv_account_master, scv_account_master_schema)

# COMMAND ----------

# DBTITLE 1,write to UC
# Build DDL string from schema dict
scv_master_ddl = ",\n".join(
    f"{col} {dtype}" for col, dtype in scv_account_master_schema.items()
)
 
try:
    # Convert DDL string to StructType
    ddl_struct = StructType.fromDDL(scv_master_ddl)
    if compare_schema_lenient(df_scv_account_master.schema, ddl_struct):
        print("Schema matches!")
        (df_scv_account_master.write.format("delta")
                        .mode("overwrite")
                        .saveAsTable(target_table))
        row_count = spark.table(target_table).count()
        print(f"SCV Master Table Loaded")
        print(f"Total Records: {row_count}")
    else:
        print("Schema mismatch!")
        displayHTML(f"<b>Expected:</b><br><pre>{ddl_struct}</pre>")
        displayHTML(f"<b>Actual:</b><br><pre>{df_scv_account_master.schema}</pre>")
        raise Exception("Schema mismatch between actual and expected.")
 
except Exception as e:
    print(f"Notebook failed: {str(e)}")
    raise e

# COMMAND ----------

# DBTITLE 1,update_latest_tag
# add last_updated tag with current timestamp to identify table last refreshed datetime.
add_last_updated_tag(target_table)

# COMMAND ----------

# DBTITLE 1,plt_logging
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
