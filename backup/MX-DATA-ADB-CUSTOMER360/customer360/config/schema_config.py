# Databricks notebook source
# DBTITLE 1,Master Table Schema
# Define the desired schemas for platinum tables
#Omni Master
omni_account_master_schema = {
    "omni_account_sk"           : "STRING",
    "account_id"                : "STRING",
    "email_id"                  : "STRING",
    "first_name"                : "STRING",
    "last_name"                 : "STRING",
    "dob"                       : "DATE",
    "gender"                    : "STRING",
    "alternate_email_id"        : "STRING",
    "mobile_country_code"       : "STRING",
    "mobile_num"                : "STRING",
    "city"                      : "STRING",
    "nationality_code"          : "STRING",
    "nationality_name"          : "STRING",
    "country_of_residence_code" : "STRING",
    "country_of_residence_name" : "STRING",
    "staff_flag"                : "STRING",
    "dependent_flag"            : "STRING",
    "pod_flag"                  : "STRING",
    "parent_omni_account_sk"    : "STRING",
    "parent_account_id"         : "STRING",
    "created_date"              : "TIMESTAMP",
    "updated_date"              : "TIMESTAMP",
    "source_name"               : "STRING",
    "etl_created_datetime"      : "TIMESTAMP",
    "etl_updated_datetime"      : "TIMESTAMP",
    "raw_data_json"             : "MAP<STRING, STRING>"
}

# SCV Master Table Schema
scv_account_master_schema = {
    "scv_account_sk"              : "STRING",
    "mdm_id"                      : "STRING",
    "yas_id"                      : "STRING",
    "email_id"                    : "STRING",
    "alternate_email_id"          : "STRING",
    "first_name"                  : "STRING",
    "last_name"                   : "STRING",
    "dob"                         : "DATE",
    "gender"                      : "STRING",
    "language"                    : "STRING",
    "role_to_parent"              : "STRING",
    "guest_type"                  : "STRING",
    "mobile_country_code"         : "STRING",
    "mobile_num"                  : "STRING",
    "city"                        : "STRING",
    "country_of_residence_code"   : "STRING",
    "country_of_residence_name"   : "STRING",
    "nationality_name"            : "STRING",
    "nationality_code"            : "STRING",
    "staff_flag"                  : "STRING",
    "pod_flag"                    : "STRING",
    "dependent_flag"              : "STRING",
    "facepass_enrolled_flag"      : "STRING",
    "active_yas_id_flag"          : "STRING",
    "parent_scv_account_sk"       : "STRING",
    "parent_yas_id"               : "STRING",
    "created_date"                : "TIMESTAMP",
    "modified_date"               : "TIMESTAMP",
    "source_name"                 : "STRING",
    "etl_created_datetime"        : "TIMESTAMP",
    "etl_updated_datetime"        : "TIMESTAMP",
    "raw_data_json"               : "MAP<STRING, STRING>"
}

# SCV Omni Link
scv_omni_link_schema = {
    "scv_omni_sk"                    : "STRING",
    "scv_account_sk"                 : "STRING",
    "omni_account_sk"                : "STRING",
    "source_name"                    : "STRING",
    "source_last_modified_datetime"  : "TIMESTAMP",
    "scv_rank"                : "BIGINT",
    "omni_rank"               : "BIGINT",
    "scv_source_rank"                : "BIGINT",
    "omni_source_rank"               : "BIGINT",
    "etl_created_datetime"           : "TIMESTAMP",
    "etl_updated_datetime"           : "TIMESTAMP"
}


# COMMAND ----------

# DBTITLE 1,Existing Data Products
# Location Intelligence
location_inteligence_schema = {
    "entity_identifiers"    : "MAP<STRING, STRING>",
    "purchaser_scv_sk"      : "STRING",
    "purchaser_omni_sk"     : "STRING",
    "owner_scv_sk"          : "STRING",
    "owner_omni_sk"         : "STRING",
    "fl_scv_sk"             : "STRING",
    "category"              : "STRING",
    "asset_name"            : "STRING",
    "checkin_timestamp"     : "TIMESTAMP",
    "checkout_timestamp"    : "TIMESTAMP",
    "annual_pass_flag"      : "STRING",
    "product_name"          : "STRING",
    "valid_sk"              : "STRING",
    "source_name"           : "STRING",
    "etl_created_datetime"  : "TIMESTAMP",
    "etl_updated_datetime"  : "TIMESTAMP",
    "others"                : "MAP<STRING, STRING>"
}

   
transactions_schema = {
    "entity_identifiers"                     : "MAP<STRING, STRING>",
    "purchaser_scv_sk"                       : "STRING",
    "purchaser_omni_sk"                      : "STRING",
    "owner_scv_sk"                           : "STRING",
    "owner_omni_sk"                          : "STRING",
    "fl_scv_sk"                              : "STRING",
    "b2b_account_id"                         : "STRING",
    "customer_type"                          : "STRING",
    "account_manager_name"                   : "STRING",
    "sale_id"                                : "STRING",
    "sale_timestamp"                         : "TIMESTAMP",
    "transaction_timestamp"                  : "TIMESTAMP",
    "sale_source_system"                     : "STRING",
    "ticket_id"                              : "STRING",
    "ticket_status"                          : "STRING",
    "annual_pass"                            : "STRING",
    "multi_park_pass"                        : "STRING",
    "ticket_tier"                            : "STRING",
    "ticket_age_category"                    : "STRING",
    "ticket_valid_from"                      : "TIMESTAMP",
    "ticket_valid_to"                        : "TIMESTAMP",
    "ticket_is_used"                         : "STRING",
    "ticket_usage_date"                      : "TIMESTAMP",
    "3rdparty_sale_product_category"         : "STRING",
    "3rdparty_sale_booking_type"             : "STRING",
    "3rdparty_sale_booking_status"           : "STRING",
    "3rdparty_sale_payment_status"           : "STRING",
    "3rdparty_sale_cancelation_date"         : "TIMESTAMP",
    "product_id"                             : "STRING",
    "product_name"                           : "STRING",
    "product_category"                       : "STRING",
    "product_subcategory"                    : "STRING",
    "park_type"                              : "STRING",
    "quantity"                               : "BIGINT",
    "unit_price"                             : "DOUBLE",
    "discounted_unit_price"                  : "DOUBLE",
    "hotel_name"                             : "STRING",
    "hotel_number_rooms"                     : "BIGINT",
    "hotel_number_nights"                    : "BIGINT",
    "hotel_length_stay"                      : "BIGINT",
    "partner_name"                           : "STRING",
    "selling_entity"                         : "STRING",
    "channel"                                : "STRING",
    "channel_area"                           : "STRING",
    "dmg_category"                           : "STRING",
    "dmg_name"                               : "STRING",
    "annual_pass_dmg"                        : "STRING",
    "promotoin_dmg"                          : "STRING",
    "promotoin_dmg_description"              : "STRING",
    "market_segment_group1"                  : "STRING",
    "market_segment_group2"                  : "STRING",
    "3rdparty_market_segment"                : "STRING",
    "3rdparty_market_region"                 : "STRING",
    "promotion_name"                         : "STRING",
    "promotion_type"                         : "STRING",
    "promotion_class"                        : "STRING",
    "promotion_from_date"                    : "TIMESTAMP",
    "promotion_to_date"                      : "TIMESTAMP",
    "complementry_item"                      : "STRING",
    "gross_sale_amount"                      : "DOUBLE",
    "discount_amount"                        : "DOUBLE",
    "net_sale_amount_no_tax"                 : "DOUBLE",
    "net_sale_amount"                        : "DOUBLE",
    "revenue_type"                           : "STRING",
    "transaction_type"                       : "STRING",
    "valid_sk"                               : "STRING",
    "source_name"                            : "STRING",
    "etl_created_datetime"                   : "TIMESTAMP",
    "etl_updated_datetime"                   : "TIMESTAMP",
    "others"                                 : "MAP<STRING, STRING>"
}

# touchpoints schema
touchpoints_schema = {
    "entity_identifiers": "MAP<STRING, STRING>",
	"owner_scv_sk" : "string",
	"owner_omni_sk" : "string",	
	"contact_profile" : "MAP<STRING, STRING>",
	"type" : "string",
    "conversation_id": "string",
    "conversation_start": "Timestamp",
    "conversation_end": "Timestamp",
    "resolution_time": "string",
    "originating_direction": "string",
    "inquiry_type": "string",
    "affected_asset": "string",
    "ivr_details": "MAP<STRING, STRING>",
    "payment_details": "MAP<STRING, STRING>",
    "flow_details": "MAP<STRING, STRING>",
    "inbound_communication_count": "string",
    "outbound_communication_count": "string",
    "total_conversation_participants": "string",
    "acd_queues_involved": "string",
    "agents_involved": "string",
    "workflows_involved": "string",
    "ivr_involved": "string",
    "voicemails_involved": "string",
    "others": "MAP<STRING, STRING>",
    "source_name": "string",
    "etl_created_datetime" : "Timestamp",
    "etl_updated_datetime": "Timestamp"
}


# CRM complaints/refunds schema
crm_complaints_refunds_schema = {
"entity_identifiers"          : "MAP<STRING, STRING>",
"owner_scv_sk"                : "STRING",
"owner_omni_sk"               : "STRING",
"email_address"               : "STRING",
"incident_id"                 : "STRING",
"modifiedon"                  : "TIMESTAMP",
"case_ticket_number"          : "STRING",
"case_source"                 : "STRING",
"case_origin"                 : "STRING",
"case_type"                   : "STRING",
"customer_id"                 : "STRING",
"case_date"                   : "TIMESTAMP",
"case_date_source"            : "STRING",
"case_title"                  : "STRING",
"case_description"            : "STRING",
"case_facility_name"          : "STRING",
"case_status"                 : "STRING",
"case_service_stage"          : "STRING",
"case_requested_refund_amount": "STRING",
"case_refund_reason"          : "STRING",
"case_payment_source"         : "STRING",
"case_date_contacted"         : "STRING",
"case_additional_notes"       : "STRING",
"case_corrective_action"      : "STRING",
"others"                     : "MAP<STRING, STRING>",
"source_name"                : "STRING",
"etl_created_datetime"       : "TIMESTAMP",
"etl_updated_datetime"       : "TIMESTAMP"
}


# Account_Contact_RFP
crm_acc_contact_rfp_schema = {
    "account_email"                        : "STRING",
    "contact_email"                        : "STRING",
    "account_id"                           : "STRING",
    "account_name"                         : "STRING",
    "account_description"                  : "STRING",
    "account_segment"                      : "STRING",
    "account_subsegment"                   : "STRING",
    "account_country"                      : "STRING",
    "account_credit_limit"                 : "STRING",
    "account_eh_travelbox_credit_limit"    : "STRING",
    "yc_accountid"                         : "STRING",

    "active_accountmgr_dmc"                : "STRING",
    "active_accountmgr_etihad_arena"       : "STRING",
    "active_accountmgr_experience_hub"     : "STRING",
    "active_accountmgr_farah_experiences"  : "STRING",
    "active_accountmgr_qasr_al_watan"      : "STRING",
    "active_accountmgr_yas_concierge"      : "STRING",
    "active_accountmgr_yascrm"             : "STRING",

    "accountmgr_history"                   : "ARRAY<STRING>",

    "dot_accountid"                        : "STRING",
    "active_contract_experience_hub"       : "STRING",
    "active_contract_farah_experiences"    : "STRING",
    "active_contract_missing_bu_1"         : "STRING",
    "active_contract_missing_bu_2"         : "STRING",
    "active_contract_yascrm"               : "STRING",

    "clientcontract_history"               : "ARRAY<STRING>",

    "contact_id"                           : "STRING",
    "contact_type"                         : "ARRAY<STRING>",
    "contact_preferred_language"           : "STRING",
    "contact_mobile"                       : "STRING",
    "contact_telephone"                    : "STRING",
    "contact_fullname"                     : "STRING",
    "crm_source_flag"                      : "STRING",

    "scv_account_sk"                       : "STRING",
    "source_name"                          : "STRING",
    "etl_created_datetime"                 : "STRING",
    "etl_updated_datetime"                 : "STRING"
}

# Customer Survey Analysis
vw_survey_analysis_schema = {
    "entity_identifiers"       : "MAP<STRING, STRING>",
    "owner_scv_sk"             : "STRING",
    "owner_omni_sk"            : "STRING",
    "response_id"              : "LONG",
    "visit_date"               : "DATE",
    "response_date"            : "DATE",
    "facility_name"            : "STRING",
    "survey_name"              : "STRING",
    "respondent_details"       : "MAP<STRING, STRING>",
    "entry_details"            : "MAP<STRING, STRING>",
    "nps_score"                : "DOUBLE",
    "gss_score"                : "DOUBLE",
    "generic_visitor_ratings"  : "MAP<STRING, STRING>",
    "park_visitor_ratings"     : "MAP<STRING, STRING>",
    "positive_comments"        : "STRING",
    "negative_comments"        : "STRING",
    "source_name"              : "STRING",
    "etl_created_datetime"     : "STRING",
    "etl_updated_datetime"     : "STRING"
}


#marketing schema
marketing_schema = {
    "entity_identifiers"             : "MAP<STRING, STRING>",
    "owner_scv_sk"                   : "STRING",
    "owner_omni_sk"                  : "STRING",
    "fact_key"                       : "BIGINT",
    "campaign_source"                : "STRING",
    "customer_email"                 : "STRING",
    "message_id"                     : "INTEGER",
    "hardware"                       : "STRING",
    "unsubscribe_flag"               : "BOOLEAN",
    "account_name"                   : "STRING",
    "event_type"                     : "STRING",
    "event_sub_type"                 : "STRING",
    "event_time"                     : "TIMESTAMP",
    "campaign_key"                   : "INTEGER",
    "campaign_name"                  : "STRING",
    "utm_tag"                        : "STRING",
    "campaign_type"                  : "STRING",
    "campaign_category"              : "STRING",
    "campaign_launch_date"           : "TIMESTAMP",
    "others"                         : "MAP<STRING, STRING>",
    "source_name"                    : "STRING",
    "etl_created_datetime"           : "TIMESTAMP",
    "etl_updated_datetime"           : "TIMESTAMP"
}

# COMMAND ----------

# DBTITLE 1,New Data Products
# Customer Interests Schema
customer_interests_schema = {
    "entity_identifiers"                 : "MAP<STRING, STRING>",
    "owner_scv_sk"                       : "STRING",
    "owner_omni_sk"                      : "STRING",
    "email"                              : "STRING",
    "yas_id"                             : "STRING",
    "lead_source"                        : "STRING",
    "guest_preference_facility_code"     : "STRING",
    "guest_preference_source_system_id"  : "STRING",
    "guest_preference_name"              : "STRING",
    "guest_preference_value"             : "STRING",
    "guest_preference_type"              : "STRING",
    "f_pref_key"                         : "STRING",
    "source_modified_on"                 : "TIMESTAMP",
    "source_name"                        : "STRING",
    "etl_created_datetime"               : "TIMESTAMP",
    "etl_updated_datetime"               : "TIMESTAMP"
}

# Customer Preference Schema
customer_preference_schema = {
    "entity_identifiers"      : "MAP<STRING, STRING>",
    "owner_scv_sk"            : "STRING",
    "owner_omni_sk"           : "STRING",
    "email"                   : "STRING",
    "preference_facility_code": "STRING",
    "channel_type"            : "STRING",
    "channel_name"            : "STRING",
    "opt_in_ind"              : "STRING",
    "f_comm_pref_key"         : "STRING",
    "comm_pref_created_date"  : "TIMESTAMP",
    "comm_pref_updated_date"  : "TIMESTAMP",
    "source_name"             : "STRING",
    "etl_created_datetime"    : "TIMESTAMP",
    "etl_updated_datetime"    : "TIMESTAMP"
}

# annual_pass schema
annual_pass_schema = {
  "entity_identifiers": "MAP<STRING, STRING>",
  "purchaser_scv_sk": "String",
  "purchaser_omni_sk": "String",
  "owner_scv_sk": "String",
  "owner_omni_sk": "String",  
  "as_of_date": "Date",
  "status": "String",
  "ticket_sale_facility_name": "String",
  "ticket_sale_facility_code": "String",
  "ticket_id": "String",
  "media_id": "String",
  "media_code": "String",
  "product_code": "String",
  "product_name": "String",
  "ticket_code": "String",
  "ticket_description": "String",
  "ticket_age": "String",
  "ticket_tier": "String",
  "rack_rate": "DOUBLE",
  "park": "String",
  "site": "String",
  "channel": "String",
  "market_segment_rule_1": "String",
  "market_segment_group_1": "String",
  "market_segment_rule_2": "String",
  "market_segment_group_2": "String",
  "omni_promotion_name": "String",
  "omni_promotion_code": "String",
  "promotion_dmg_description": "String",
  "promotion_flag": "String",
  "promotion_valid_from": "TIMESTAMP",
  "promotion_valid_to": "TIMESTAMP",
  "void_type": "String",
  "void_type_description": "String",
  "workstation_id": "String",
  "workstation_area": "String",
  "upgrade_downgrade": "String",
  "upgrade_downgrade_date": "TIMESTAMP",
  "acquisition_reacquisition": "String",
  "ticket_active_from": "TIMESTAMP",
  "ticket_active_to": "TIMESTAMP",
  "sale_date_time": "TIMESTAMP",
  "voucher_name": "String",
  "unit_price_before_discount": "DOUBLE",
  "unit_price_after_discount": "DOUBLE",
  "quantity": "BIGINT",
  "gross_sale_amount": "DOUBLE",
  "discount_amount": "DOUBLE",
  "tax_amount": "DOUBLE",
  "net_sale_amount": "DOUBLE",
  "net_sale_amount_wo_tax": "DOUBLE",
  "others" : "MAP<STRING, STRING>",
  "source_name": "string",
  "etl_created_datetime" : "Timestamp",
  "etl_updated_datetime": "Timestamp"
}





# COMMAND ----------

# DBTITLE 1,KPI
# Master KPI Table Schema
kpi_master_schema = {
    "kpi_id"          : "BIGINT",
    "kpi_name"        : "STRING",
    "kpi_description" : "STRING",
    "active_flag"     : "BOOLEAN",
    "kpi_level"       : "STRING",
    "source_table"    : "STRING", 
    "created_at"      : "TIMESTAMP",
    "modified_at"     : "TIMESTAMP"
}

# Customer KPI Schema
customer_kpi_schema = {
    "report_date"      : "DATE",
    "report_hour"      : "INTEGER",
    "identifier_type"  : "STRING",
    "identifier_value" : "STRING",
    "kpi_id"           : "BIGINT",
    "kpi_value"        : "STRING",
    "created_at"       : "TIMESTAMP",
    "modified_at"      : "TIMESTAMP"
}

# Asset KPI Schema
asset_kpi_schema = {
    "report_date" : "DATE",
    "report_hour" : "INTEGER",
    "asset_name" : "STRING",
    "kpi_id" : "BIGINT",
    "kpi_value" : "STRING",
    "etl_created_datetime" : "TIMESTAMP",
    "etl_updated_datetime" : "TIMESTAMP"
}

# Group KPI Schema
group_kpis_schema = {
    "report_date" : "DATE",
    "report_hour" : "INTEGER",
    "group_name"  : "STRING",
    "kpi_id"      : "BIGINT",
    "kpi_value"   : "STRING",
    "created_at"  : "TIMESTAMP",
    "modified_at" : "TIMESTAMP"
}
