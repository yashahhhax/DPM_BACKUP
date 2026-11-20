# Databricks notebook source
dbutils.widgets.text("environment", "")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

transformation_catalog = f"transformation_{environment}"
spark.sql(f"use catalog {transformation_catalog}")

create_workflow_dependencies_table(transformation_catalog)

# COMMAND ----------

# DBTITLE 1,shared
update_workflow_dependencies(
    transformation_catalog,
    'shared_maintenance, shared_ea_drr',
    'wf_dwh_dim_employee',
    [
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_emp_mast_all"},
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_dept_master_all"},
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_emp_service_dtls_all"},
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_people_dtls_all"},
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_position_master_all"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_travelbox,shared_ea_drr',
    'wf_dwh_dim_country',
    [
        {"table_name" : "brz_crm.vw_crm_dot_country"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_maintenance,shared_clymb_drr,shared_ig_drr,shared_omni_drr,shared_ea_drr,shared_ridecollation,shared_gss',
    'wf_dwh_dim_company',
    [
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_companydimension"},
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_coa_segment_mapping"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_travelbox,shared_ig_drr,shared_omni_drr',
    'wf_dwh_dim_b2c_account',
    [
        {"table_name" : "brz_omni.vw_dbo_vw_accounts"},
        {"table_name" : "brz_scv.vw_dbo_t_mdm_guest"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_travelbox,shared_ig_drr,shared_omni_drr',
    'wf_dwh_dim_b2b_partner',
    [
        {"table_name" : "brz_crm.vw_crm_dot_country"},
        {"table_name" : "brz_crm.vw_crm_dot_region"},
        {"table_name" : "brz_crm.vw_crm_dot_segment"},
        {"table_name" : "brz_crm.vw_crm_account"},
        {"table_name" : "brz_crm.vw_crm_fe_omnidmgcategory"},
        {"table_name" : "brz_omni.vw_dbo_data_account"},
        {"table_name" : "brz_omni.vw_dbo_cnf_dmgcategory"},
        {"table_name" : "brz_omni.vw_dbo_data_dmgdata"}
    ]
)
update_workflow_dependencies(
    transformation_catalog,
    'shared_clymb_drr,shared_ea_drr,shared_ig_drr,shared_omni_drr,shared_maintenance',
    'wf_dwh_lkp_source_system',
    [
        {"table_name": "brz_sharepoint_list.vw_farahflfin_drr_source_systems"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_clymb_drr,shared_omni_drr,shared_ig_drr,shared_ea_drr',
    'wf_dwh_lkp_season',
    [
        {"table_name": "brz_sharepoint_list.vw_farahflms_seasons"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_ea_drr,shared_omni_drr,shared_ig_drr',
    'wf_dwh_dim_sales_account_manager',
    [
        {"table_name" : "brz_crm.vw_crm_account"},
        {"table_name" : "brz_crm.vw_crm_dot_segment"},
        {"table_name" : "brz_crm.vw_crm_yc_accountmanagerhistory"},
        {"table_name" : "brz_crm.vw_crm_dot_accountbusinessunitprofile"},
        {"table_name" : "brz_crm.vw_crm_systemuser"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_ea_drr,shared_ig_drr,shared_omni_drr',
    'wf_dwh_dim_market_segments',
    [
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_market_segments"}
    ]
)


update_workflow_dependencies(
    transformation_catalog,
    'shared_ea_drr,shared_clymb_drr,shared_ig_drr,shared_omni_drr',
    'wf_dwh_lkp_transaction_type',
    [
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_revenue_transaction_type"}
    ]
)


update_workflow_dependencies(
    transformation_catalog,
    'shared_ea_drr,shared_clymb_drr,shared_ig_drr,shared_omni_drr',
    'wf_dwh_dim_department',
    [
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_coa_dept_master_all"},
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_coa_segment_mapping"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_ea_drr,shared_oracle_gl',
    'wf_dwh_lkp_coa_segment_mapping',
    [
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_coa_segment_mapping"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_ea_drr,shared_oracle_gl,shared_clymb_drr,shared_ig_drr,shared_omni_drr',
    'wf_dwh_dim_gl_account',
    [
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_coa_acct_master_all"},
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_coa_segment_mapping"},
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_acct_map_hdr_all"},
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_acct_map_dtl_all"},
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_glgroupmapping"}
    ]
)


update_workflow_dependencies(
    transformation_catalog,
    'shared_ea_drr,shared_ig_drr,shared_omni_drr,shared_clymb_drr',
    'wf_dwh_dim_sub_analysis',
    [
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflpm_bi_coa_sa_master_all"},
        {"table_name" : "brz_fusion.vw_fusion_xxapps_xxflp_omni_dmg_cat_sa_map_all"},
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_outletmapping"},
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_coa_segment_mapping"}
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    'shared_travelbox,shared_drr',
    'wf_dwh_dim_master_hotel_name',
    [
        {"table_name" : "brz_sharepoint_list.vw_farahflfin_hotelsmasterlist"}
    ]
)

# COMMAND ----------

# DBTITLE 1,duplicate check
# Read workflow_dependencies table
df = spark.sql(f"SELECT * FROM {transformation_catalog}.config.workflow_dependencies")

# Check for duplicates based on use_case, workflow_name, and table_name
duplicate_df = (
    df.groupBy("use_case", "workflow_name", "table_name")
      .count()
      .filter("count > 1")
)

# If duplicates found, raise exception
if duplicate_df.count() > 0:
    duplicate_df.show(truncate=False)
    raise Exception("Duplicate found.")
else:
    print("No duplicates found. Proceeding with execution.")
