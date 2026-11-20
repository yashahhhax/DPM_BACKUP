# Databricks notebook source
# DBTITLE 1,include library
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,define valiables
dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

dbutils.widgets.text("usecase", "")
usecase = dbutils.widgets.get("usecase")

# COMMAND ----------

# DBTITLE 1,seting up the catelog and table schema
# Set catalog
transformation_catalog = f"transformation_{env}"
spark.sql(f"use catalog {transformation_catalog}")

create_workflow_dependencies_table(transformation_catalog)

# COMMAND ----------

# DBTITLE 1,create all readiness table
readiness_tables = [
    #transactions
    "readiness_transactions_clymb",
    "readiness_transactions_ig",
    "readiness_transactions_omni",
    #location intelligence
    "readiness_location_intelligence_attendance",
    "readiness_location_intelligence_facepass",
    "readiness_location_intelligence_hotel",
    "readiness_location_intelligence_wifi",
    #customer kpi
    "readiness_customer_kpi_email_id",
    "readiness_customer_kpi_omni_sk",
    "readiness_customer_kpi_scv_sk",
    #group kpi
    "readiness_group_kpi",
    #asset kpi
    "readiness_asset_kpi",
]

for table in readiness_tables:
    table_name = f"{transformation_catalog}.plt_customer360.{table}"
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} Already exists.")
    else:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS  {table_name} (
                last_updated_timestamp TIMESTAMP
            ) USING DELTA
        """)
        print(f"Table {table_name} created.")

# COMMAND ----------

# DBTITLE 1,define workflow dependency
#SCV Master
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_scv_account_master',
    [
        {"table_name" : "brz_scv.vw_dbo_t_mdm_guest"},
        {"table_name" : "gld_drr.tbvw_attendance"},
        # {"table_name" : "gld_facepassreporting.gld_lkp_t_mdm_rel"},
        # {"table_name" : "gld_facepassreporting.gld_lkp_guest_relationships"},
        # {"table_name" : "slv_facepassreporting.slv_lkp_frictionless_data_account_snapshot"},
        # {"table_name" : "gld_facepassreporting.gld_lkp_portrait_info"},
    ]
)
#Omni Master
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_omni_account_master',
    [
        {"table_name" : "gld_drr.dwh_lkp_vw_account"}
    ]
)
#Link Table
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_scv_omni_link',
    [
        # {"table_name" : "slv_facepassreporting.slv_lkp_frictionless_data_tickets_snapshot"},
        {"table_name" : "gld_drr.dwh_lkp_vw_account"},
        {"table_name" : "gld_drr.tbvw_sales_transactions"},
        {"table_name":  "plt_customer360.scv_account_master"}
    ]
)

# Customer Interests
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_customer_interests',
    [
        {"table_name" : "gld_scv.dwh_scv_guest"},
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)

# Customer Preference
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_customer_preference',
    [
        {"table_name" : "brz_scv.vw_dwh_mdm_f_communication_preference"},
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)

# Marketing
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_marketing',
    [
        {"table_name" : "gld_emarsys.dwh_fact_email"},
        {"table_name" : "gld_emarsys.dwh_fact_push_notification"},
        {"table_name" : "gld_emarsys.dwh_dim_contact"},
        {"table_name" : "gld_emarsys.dwh_dim_emarsys_account"},
        {"table_name" : "gld_emarsys.dwh_dim_email_campaign"},
        {"table_name" : "gld_emarsys.dwh_dim_push_campaign"},
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)

# Touchpoints
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_touchpoints',
    [
        {"table_name" : "brz_genesys.vw_customer360_voice_conversations"}, 
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)

# Annual Pass
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_annual_pass',
    [
        {"table_name" : "slv_drr.stg_ap_ticketing_snapshot"}, #gld_drr.vw_dwh_fact_annual_pass
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)


# complaints_refunds
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_complaints_refunds',
    [
        {"table_name" : "brz_crm.vw_crm_dot_property"},
        {"table_name" : "brz_crm.vw_crm_incident"},
        {"table_name": "brz_crm.vw_crm_account"},
        {"table_name": "brz_crm.vw_crm_stringmap"},
        {"table_name": "brz_crm.vw_crm_businessunit"},
        {"table_name": "brz_crm.vw_crm_contact"},
        {"table_name": "brz_crm.vw_crm_systemuser"},
        {"table_name": "brz_crm.vw_crm_yc_accountmanagerhistory"},
        {"table_name": "brz_crm.vw_crm_dot_clientcontract"},
        {"table_name": "brz_crm.vw_crm_dot_segment"},
        {"table_name": "brz_crm.vw_crm_dot_country"},
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)

# customer_survey
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_customer_survey',
    [
        {"table_name": "gld_gss.dwh_fact_survey"}, #gld_gss.vw_survey_agg_analysis_all_parks
        {"table_name": "gld_gss.dwh_fact_inc_survey"},
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)

# Location Intelligence
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_location_intelligence_attendance',
    [
        {"table_name": "gld_travelbox.dwh_fact_booking"}, #gld_travelbox.dwh_vw_sales
        {"table_name": "gld_drr.tbvw_attendance"},
        # {"table_name" : "slv_facepassreporting.slv_lkp_frictionless_data_tickets_snapshot"},
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_location_intelligence_hotel',
    [
        {"table_name": "gld_travelbox.dwh_fact_booking"}, #gld_travelbox.dwh_vw_sales
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_location_intelligence_wifi',
    [
        {"table_name": "gld_ipera.dwh_dim_wifi_guest_details"},
        {"table_name": "gld_ipera.dwh_fact_wifi_guest_activities"},
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)

update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_location_intelligence_facepass',
    [
        {"table_name": "gld_facepassreporting.gld_lkp_portrait_info"},
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
    ]
)
#Transactions
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_transactions',
    [
        {"table_name": "gld_drr.tbvw_sales_transactions"},
        {"table_name": "gld_drr.dwh_lkp_vw_account"},
        {"table_name": "gld_travelbox.dwh_fact_booking"}, #gld_travelbox.dwh_vw_sales
        # {"table_name" : "slv_facepassreporting.slv_lkp_frictionless_data_tickets_snapshot"},
        {"table_name" : "plt_customer360.scv_account_master"},
        {"table_name" : "plt_customer360.omni_account_master"},
        {"table_name" : "plt_customer360.scv_omni_link"},
    ]
)

#Master KPI Table
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_master_kpi',
    [
        {"table_name": "brz_sharepoint_file.vw_customer360_master_kpi_list"},
    ]
)


#KPI workflow
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_kpi_refresh',
    [
        {"table_name": "plt_customer360.readiness_transactions_clymb","hard_dependency": False},
        {"table_name": "plt_customer360.readiness_transactions_ig","hard_dependency": False},
        {"table_name": "plt_customer360.readiness_transactions_omni","hard_dependency": False},
        # {"table_name": "plt_customer360.readiness_location_intelligence_facepass","hard_dependency": False},
        {"table_name": "plt_customer360.readiness_location_intelligence_attendance","hard_dependency": False},
        {"table_name": "plt_customer360.readiness_location_intelligence_hotel","hard_dependency": False},
        {"table_name": "plt_customer360.readiness_location_intelligence_wifi","hard_dependency": False},
        {"table_name": "plt_customer360.marketing","hard_dependency": False},
    ]
)

#executive summary
update_workflow_dependencies(
    transformation_catalog,
    usecase,
    'wf_c360_master_email_summary',
    [
        {"table_name": "plt_customer360.readiness_customer_kpi_email_id","hard_dependency": False},
        {"table_name": "plt_customer360.readiness_customer_kpi_omni_sk","hard_dependency": False},
        {"table_name": "plt_customer360.readiness_group_kpi","hard_dependency": False},
        {"table_name": "plt_customer360.readiness_asset_kpi","hard_dependency": False},
        {"table_name": "plt_customer360.touchpoints","hard_dependency": False},
        {"table_name": "plt_customer360.customer_survey","hard_dependency": False},
        {"table_name": "plt_customer360.customer_preference","hard_dependency": False},
        {"table_name": "plt_customer360.customer_interests","hard_dependency": False},
        {"table_name": "plt_customer360.crm_account_contacts_rfp","hard_dependency": False},
        {"table_name": "plt_customer360.complaints_and_refunds","hard_dependency": False},
        {"table_name": "plt_customer360.annual_pass","hard_dependency": False},
    ]
)


print(f"{usecase} dependencies registered successfully.")
