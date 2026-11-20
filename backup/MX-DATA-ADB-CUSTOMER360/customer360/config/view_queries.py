# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,C360 View Query Definitions
c360_view_queries = {

    # ============================================================
    # vw_location_intelligence
    # ============================================================
    "vw_location_intelligence": """
    SELECT
        entity_identifiers.email_id AS email_entity_identifier

        -- SCV details
        ,IFF(entity_identifiers.scv_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details
        ,IFF(entity_identifiers.omni_sk IS NOT NULL, MAP(
            'omni_account_sk',        CAST(omn_mst_1.omni_account_sk AS STRING)
            ,'account_id',            CAST(omn_mst_1.account_id AS STRING)
            ,'email_id',              CAST(omn_mst_1.email_id AS STRING)
            ,'first_name',            CAST(omn_mst_1.first_name AS STRING)
            ,'last_name',             CAST(omn_mst_1.last_name AS STRING)
            ,'dob',                   CAST(omn_mst_1.dob AS STRING)
            ,'gender',                CAST(omn_mst_1.gender AS STRING)
            ,'mobile_country_code',   CAST(omn_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(omn_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(omn_mst_1.city AS STRING)
            ,'nationality_name',      CAST(omn_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(omn_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(omn_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(omn_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(omn_mst_1.pod_flag AS STRING)
            ,'parent_omni_account_sk', CAST(omn_mst_1.parent_omni_account_sk AS STRING)
            ,'parent_account_id',     CAST(omn_mst_1.parent_account_id AS STRING)
        ), NULL) AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'purchaser_scv_sk',  CAST(purchaser_scv_sk AS STRING)
            ,'purchaser_omni_sk', CAST(purchaser_omni_sk AS STRING)
            ,'owner_scv_sk',     CAST(owner_scv_sk AS STRING)
            ,'owner_omni_sk',    CAST(owner_omni_sk AS STRING)
            ,'fl_scv_sk',        CAST(fl_scv_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,location_intelligence.category
        ,location_intelligence.asset_name
        ,location_intelligence.checkin_timestamp
        ,location_intelligence.checkout_timestamp
        ,location_intelligence.annual_pass_flag
        ,location_intelligence.product_name
        ,location_intelligence.valid_sk
        ,location_intelligence.source_name
        ,location_intelligence.others

    FROM {catalog}.{schema}.location_intelligence
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON entity_identifiers.scv_sk = scv_mst_1.scv_account_sk
    LEFT JOIN {catalog}.{schema}.omni_account_master omn_mst_1
        ON entity_identifiers.omni_sk = omn_mst_1.omni_account_sk
    """,

    # ============================================================
    # vw_transactions
    # ============================================================
    "vw_transactions": """
    SELECT
        transactions.entity_identifiers.email_id AS email_entity_identifier

        -- SCV details
        ,IFF(transactions.entity_identifiers.scv_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details
        ,IFF(transactions.entity_identifiers.omni_sk IS NOT NULL, MAP(
            'omni_account_sk',        CAST(omn_mst_1.omni_account_sk AS STRING)
            ,'account_id',            CAST(omn_mst_1.account_id AS STRING)
            ,'email_id',              CAST(omn_mst_1.email_id AS STRING)
            ,'first_name',            CAST(omn_mst_1.first_name AS STRING)
            ,'last_name',             CAST(omn_mst_1.last_name AS STRING)
            ,'dob',                   CAST(omn_mst_1.dob AS STRING)
            ,'gender',                CAST(omn_mst_1.gender AS STRING)
            ,'mobile_country_code',   CAST(omn_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(omn_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(omn_mst_1.city AS STRING)
            ,'nationality_name',      CAST(omn_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(omn_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(omn_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(omn_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(omn_mst_1.pod_flag AS STRING)
            ,'parent_omni_account_sk', CAST(omn_mst_1.parent_omni_account_sk AS STRING)
            ,'parent_account_id',     CAST(omn_mst_1.parent_account_id AS STRING)
        ), NULL) AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'purchaser_scv_sk',  CAST(transactions.purchaser_scv_sk AS STRING)
            ,'purchaser_omni_sk', CAST(transactions.purchaser_omni_sk AS STRING)
            ,'owner_scv_sk',     CAST(transactions.owner_scv_sk AS STRING)
            ,'owner_omni_sk',    CAST(transactions.owner_omni_sk AS STRING)
            ,'fl_scv_sk',        CAST(transactions.fl_scv_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,transactions.b2b_account_id
        ,transactions.customer_type
        ,transactions.account_manager_name
        ,transactions.sale_id
        ,transactions.sale_timestamp
        ,transactions.transaction_timestamp
        ,transactions.sale_source_system
        ,transactions.ticket_id
        ,transactions.ticket_status
        ,transactions.annual_pass
        ,transactions.multi_park_pass
        ,transactions.ticket_tier
        ,transactions.ticket_age_category
        ,transactions.ticket_valid_from
        ,transactions.ticket_valid_to
        ,transactions.ticket_is_used
        ,transactions.ticket_usage_date
        ,transactions.3rdparty_sale_product_category
        ,transactions.3rdparty_sale_booking_type
        ,transactions.3rdparty_sale_booking_status
        ,transactions.3rdparty_sale_payment_status
        ,transactions.3rdparty_sale_cancelation_date
        ,transactions.product_id
        ,transactions.product_name
        ,transactions.product_category
        ,transactions.product_subcategory
        ,transactions.park_type
        ,transactions.quantity
        ,transactions.unit_price
        ,transactions.discounted_unit_price
        ,transactions.hotel_name
        ,transactions.hotel_number_rooms
        ,transactions.hotel_number_nights
        ,transactions.hotel_length_stay
        ,transactions.partner_name
        ,transactions.selling_entity
        ,transactions.channel
        ,transactions.channel_area
        ,transactions.dmg_category
        ,transactions.dmg_name
        ,transactions.annual_pass_dmg
        ,transactions.promotoin_dmg
        ,transactions.promotoin_dmg_description
        ,transactions.market_segment_group1
        ,transactions.market_segment_group2
        ,transactions.3rdparty_market_segment
        ,transactions.3rdparty_market_region
        ,transactions.promotion_name
        ,transactions.promotion_type
        ,transactions.promotion_class
        ,transactions.promotion_from_date
        ,transactions.promotion_to_date
        ,transactions.complementry_item
        ,transactions.gross_sale_amount
        ,transactions.discount_amount
        ,transactions.net_sale_amount_no_tax
        ,transactions.net_sale_amount
        ,transactions.revenue_type
        ,transactions.transaction_type
        ,transactions.others

    FROM {catalog}.{schema}.transactions transactions
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON transactions.entity_identifiers.scv_sk = scv_mst_1.scv_account_sk
    LEFT JOIN {catalog}.{schema}.omni_account_master omn_mst_1
        ON transactions.entity_identifiers.omni_sk = omn_mst_1.omni_account_sk
    """,

    # ============================================================
    # vw_annual_pass
    # ============================================================
    "vw_annual_pass": """
    SELECT
        annual_pass.entity_identifiers.email_id AS email_entity_identifier

        -- SCV details
        ,IFF(annual_pass.entity_identifiers.scv_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details
        ,IFF(annual_pass.entity_identifiers.omni_sk IS NOT NULL, MAP(
            'omni_account_sk',        CAST(omn_mst_1.omni_account_sk AS STRING)
            ,'account_id',            CAST(omn_mst_1.account_id AS STRING)
            ,'email_id',              CAST(omn_mst_1.email_id AS STRING)
            ,'first_name',            CAST(omn_mst_1.first_name AS STRING)
            ,'last_name',             CAST(omn_mst_1.last_name AS STRING)
            ,'dob',                   CAST(omn_mst_1.dob AS STRING)
            ,'gender',                CAST(omn_mst_1.gender AS STRING)
            ,'mobile_country_code',   CAST(omn_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(omn_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(omn_mst_1.city AS STRING)
            ,'nationality_name',      CAST(omn_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(omn_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(omn_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(omn_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(omn_mst_1.pod_flag AS STRING)
            ,'parent_omni_account_sk', CAST(omn_mst_1.parent_omni_account_sk AS STRING)
            ,'parent_account_id',     CAST(omn_mst_1.parent_account_id AS STRING)
        ), NULL) AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'purchaser_scv_sk',  CAST(annual_pass.purchaser_scv_sk AS STRING)
            ,'purchaser_omni_sk', CAST(annual_pass.purchaser_omni_sk AS STRING)
            ,'owner_scv_sk',     CAST(annual_pass.owner_scv_sk AS STRING)
            ,'owner_omni_sk',    CAST(annual_pass.owner_omni_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,annual_pass.as_of_date
        ,annual_pass.status
        ,annual_pass.ticket_sale_facility_name
        ,annual_pass.ticket_sale_facility_code
        ,annual_pass.ticket_id
        ,annual_pass.media_id
        ,annual_pass.media_code
        ,annual_pass.product_code
        ,annual_pass.product_name
        ,annual_pass.ticket_code
        ,annual_pass.ticket_description
        ,annual_pass.ticket_age
        ,annual_pass.ticket_tier
        ,annual_pass.rack_rate
        ,annual_pass.park
        ,annual_pass.site
        ,annual_pass.channel
        ,annual_pass.market_segment_rule_1
        ,annual_pass.market_segment_group_1
        ,annual_pass.market_segment_rule_2
        ,annual_pass.market_segment_group_2
        ,annual_pass.omni_promotion_name
        ,annual_pass.omni_promotion_code
        ,annual_pass.promotion_dmg_description
        ,annual_pass.promotion_flag
        ,annual_pass.promotion_valid_from
        ,annual_pass.promotion_valid_to
        ,annual_pass.void_type
        ,annual_pass.void_type_description
        ,annual_pass.workstation_id
        ,annual_pass.workstation_area
        ,annual_pass.upgrade_downgrade
        ,annual_pass.upgrade_downgrade_date
        ,annual_pass.acquisition_reacquisition
        ,annual_pass.ticket_active_from
        ,annual_pass.ticket_active_to
        ,annual_pass.sale_date_time
        ,annual_pass.voucher_name
        ,annual_pass.unit_price_before_discount
        ,annual_pass.unit_price_after_discount
        ,annual_pass.quantity
        ,annual_pass.gross_sale_amount
        ,annual_pass.discount_amount
        ,annual_pass.tax_amount
        ,annual_pass.net_sale_amount
        ,annual_pass.net_sale_amount_wo_tax
        ,annual_pass.others
        ,annual_pass.source_name

    FROM {catalog}.{schema}.annual_pass annual_pass
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON annual_pass.entity_identifiers.scv_sk = scv_mst_1.scv_account_sk
    LEFT JOIN {catalog}.{schema}.omni_account_master omn_mst_1
        ON annual_pass.entity_identifiers.omni_sk = omn_mst_1.omni_account_sk
    """,

    # ============================================================
    # vw_complaints_and_refunds
    # ============================================================
    "vw_complaints_and_refunds": """
    SELECT
        complaints.entity_identifiers.email_id AS email_entity_identifier

        -- SCV details
        ,IFF(complaints.entity_identifiers.scv_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details
        ,IFF(complaints.entity_identifiers.omni_sk IS NOT NULL, MAP(
            'omni_account_sk',        CAST(omn_mst_1.omni_account_sk AS STRING)
            ,'account_id',            CAST(omn_mst_1.account_id AS STRING)
            ,'email_id',              CAST(omn_mst_1.email_id AS STRING)
            ,'first_name',            CAST(omn_mst_1.first_name AS STRING)
            ,'last_name',             CAST(omn_mst_1.last_name AS STRING)
            ,'dob',                   CAST(omn_mst_1.dob AS STRING)
            ,'gender',                CAST(omn_mst_1.gender AS STRING)
            ,'mobile_country_code',   CAST(omn_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(omn_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(omn_mst_1.city AS STRING)
            ,'nationality_name',      CAST(omn_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(omn_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(omn_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(omn_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(omn_mst_1.pod_flag AS STRING)
            ,'parent_omni_account_sk', CAST(omn_mst_1.parent_omni_account_sk AS STRING)
            ,'parent_account_id',     CAST(omn_mst_1.parent_account_id AS STRING)
        ), NULL) AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'owner_scv_sk',  CAST(complaints.owner_scv_sk AS STRING)
            ,'owner_omni_sk', CAST(complaints.owner_omni_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,complaints.email_address
        ,complaints.incident_id
        ,complaints.modifiedon
        ,complaints.case_ticket_number
        ,complaints.case_source
        ,complaints.case_origin
        ,complaints.case_type
        ,complaints.customer_id
        ,complaints.case_date
        ,complaints.case_date_source
        ,complaints.case_title
        ,complaints.case_description
        ,complaints.case_facility_name
        ,complaints.case_status
        ,complaints.case_service_stage
        ,complaints.case_requested_refund_amount
        ,complaints.case_refund_reason
        ,complaints.case_payment_source
        ,complaints.case_date_contacted
        ,complaints.case_additional_notes
        ,complaints.case_corrective_action
        ,complaints.others
        ,complaints.source_name

    FROM {catalog}.{schema}.complaints_and_refunds complaints
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON complaints.entity_identifiers.scv_sk = scv_mst_1.scv_account_sk
    LEFT JOIN {catalog}.{schema}.omni_account_master omn_mst_1
        ON complaints.entity_identifiers.omni_sk = omn_mst_1.omni_account_sk
    """,

    # ============================================================
    # vw_crm_account_contacts_rfp
    # ============================================================
    "vw_crm_account_contacts_rfp": """
    SELECT
        crm.contact_email AS email_entity_identifier

        -- SCV details
        ,IFF(crm.scv_account_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details (not applicable for this table)
        ,NULL AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'scv_account_sk', CAST(crm.scv_account_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,crm.account_email
        ,crm.contact_email
        ,crm.account_id
        ,crm.account_name
        ,crm.account_description
        ,crm.account_segment
        ,crm.account_subsegment
        ,crm.account_country
        ,crm.account_credit_limit
        ,crm.account_eh_travelbox_credit_limit
        ,crm.yc_accountid
        ,crm.active_accountmgr_dmc
        ,crm.active_accountmgr_etihad_arena
        ,crm.active_accountmgr_experience_hub
        ,crm.active_accountmgr_farah_experiences
        ,crm.active_accountmgr_qasr_al_watan
        ,crm.active_accountmgr_yas_concierge
        ,crm.active_accountmgr_yascrm
        ,crm.accountmgr_history
        ,crm.dot_accountid
        ,crm.active_contract_experience_hub
        ,crm.active_contract_farah_experiences
        ,crm.active_contract_missing_bu_1
        ,crm.active_contract_missing_bu_2
        ,crm.active_contract_yascrm
        ,crm.clientcontract_history
        ,crm.contact_id
        ,crm.contact_type
        ,crm.contact_preferred_language
        ,crm.contact_mobile
        ,crm.contact_telephone
        ,crm.contact_fullname
        ,crm.crm_source_flag
        ,crm.source_name

    FROM {catalog}.{schema}.crm_account_contacts_rfp crm
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON crm.scv_account_sk = scv_mst_1.scv_account_sk
    """,

    # ============================================================
    # vw_customer_interests
    # ============================================================
    "vw_customer_interests": """
    SELECT
        cust_int.entity_identifiers.email_id AS email_entity_identifier

        -- SCV details
        ,IFF(cust_int.entity_identifiers.scv_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details
        ,IFF(cust_int.entity_identifiers.omni_sk IS NOT NULL, MAP(
            'omni_account_sk',        CAST(omn_mst_1.omni_account_sk AS STRING)
            ,'account_id',            CAST(omn_mst_1.account_id AS STRING)
            ,'email_id',              CAST(omn_mst_1.email_id AS STRING)
            ,'first_name',            CAST(omn_mst_1.first_name AS STRING)
            ,'last_name',             CAST(omn_mst_1.last_name AS STRING)
            ,'dob',                   CAST(omn_mst_1.dob AS STRING)
            ,'gender',                CAST(omn_mst_1.gender AS STRING)
            ,'mobile_country_code',   CAST(omn_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(omn_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(omn_mst_1.city AS STRING)
            ,'nationality_name',      CAST(omn_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(omn_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(omn_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(omn_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(omn_mst_1.pod_flag AS STRING)
            ,'parent_omni_account_sk', CAST(omn_mst_1.parent_omni_account_sk AS STRING)
            ,'parent_account_id',     CAST(omn_mst_1.parent_account_id AS STRING)
        ), NULL) AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'owner_scv_sk',  CAST(cust_int.owner_scv_sk AS STRING)
            ,'owner_omni_sk', CAST(cust_int.owner_omni_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,cust_int.email
        ,cust_int.yas_id
        ,cust_int.lead_source
        ,cust_int.guest_preference_facility_code
        ,cust_int.guest_preference_source_system_id
        ,cust_int.guest_preference_name
        ,cust_int.guest_preference_value
        ,cust_int.guest_preference_type
        ,cust_int.f_pref_key
        ,cust_int.source_modified_on
        ,cust_int.source_name

    FROM {catalog}.{schema}.customer_interests cust_int
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON cust_int.entity_identifiers.scv_sk = scv_mst_1.scv_account_sk
    LEFT JOIN {catalog}.{schema}.omni_account_master omn_mst_1
        ON cust_int.entity_identifiers.omni_sk = omn_mst_1.omni_account_sk
    """,

    # ============================================================
    # vw_customer_preference
    # ============================================================
    "vw_customer_preference": """
    SELECT
        cust_pref.entity_identifiers.email_id AS email_entity_identifier

        -- SCV details
        ,IFF(cust_pref.entity_identifiers.scv_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details
        ,IFF(cust_pref.entity_identifiers.omni_sk IS NOT NULL, MAP(
            'omni_account_sk',        CAST(omn_mst_1.omni_account_sk AS STRING)
            ,'account_id',            CAST(omn_mst_1.account_id AS STRING)
            ,'email_id',              CAST(omn_mst_1.email_id AS STRING)
            ,'first_name',            CAST(omn_mst_1.first_name AS STRING)
            ,'last_name',             CAST(omn_mst_1.last_name AS STRING)
            ,'dob',                   CAST(omn_mst_1.dob AS STRING)
            ,'gender',                CAST(omn_mst_1.gender AS STRING)
            ,'mobile_country_code',   CAST(omn_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(omn_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(omn_mst_1.city AS STRING)
            ,'nationality_name',      CAST(omn_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(omn_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(omn_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(omn_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(omn_mst_1.pod_flag AS STRING)
            ,'parent_omni_account_sk', CAST(omn_mst_1.parent_omni_account_sk AS STRING)
            ,'parent_account_id',     CAST(omn_mst_1.parent_account_id AS STRING)
        ), NULL) AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'owner_scv_sk',  CAST(cust_pref.owner_scv_sk AS STRING)
            ,'owner_omni_sk', CAST(cust_pref.owner_omni_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,cust_pref.email
        ,cust_pref.preference_facility_code
        ,cust_pref.channel_type
        ,cust_pref.channel_name
        ,cust_pref.opt_in_ind
        ,cust_pref.f_comm_pref_key
        ,cust_pref.comm_pref_created_date
        ,cust_pref.comm_pref_updated_date
        ,cust_pref.source_name

    FROM {catalog}.{schema}.customer_preference cust_pref
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON cust_pref.entity_identifiers.scv_sk = scv_mst_1.scv_account_sk
    LEFT JOIN {catalog}.{schema}.omni_account_master omn_mst_1
        ON cust_pref.entity_identifiers.omni_sk = omn_mst_1.omni_account_sk
    """,

    # ============================================================
    # vw_customer_survey
    # ============================================================
    "vw_customer_survey": """
    SELECT
        cust_survey.entity_identifiers.email_id AS email_entity_identifier

        -- SCV details
        ,IFF(cust_survey.entity_identifiers.scv_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details
        ,IFF(cust_survey.entity_identifiers.omni_sk IS NOT NULL, MAP(
            'omni_account_sk',        CAST(omn_mst_1.omni_account_sk AS STRING)
            ,'account_id',            CAST(omn_mst_1.account_id AS STRING)
            ,'email_id',              CAST(omn_mst_1.email_id AS STRING)
            ,'first_name',            CAST(omn_mst_1.first_name AS STRING)
            ,'last_name',             CAST(omn_mst_1.last_name AS STRING)
            ,'dob',                   CAST(omn_mst_1.dob AS STRING)
            ,'gender',                CAST(omn_mst_1.gender AS STRING)
            ,'mobile_country_code',   CAST(omn_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(omn_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(omn_mst_1.city AS STRING)
            ,'nationality_name',      CAST(omn_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(omn_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(omn_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(omn_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(omn_mst_1.pod_flag AS STRING)
            ,'parent_omni_account_sk', CAST(omn_mst_1.parent_omni_account_sk AS STRING)
            ,'parent_account_id',     CAST(omn_mst_1.parent_account_id AS STRING)
        ), NULL) AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'owner_scv_sk',  CAST(cust_survey.owner_scv_sk AS STRING)
            ,'owner_omni_sk', CAST(cust_survey.owner_omni_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,cust_survey.response_id
        ,cust_survey.visit_date
        ,cust_survey.response_date
        ,cust_survey.facility_name
        ,cust_survey.survey_name
        ,cust_survey.respondent_details
        ,cust_survey.entry_details
        ,cust_survey.nps_score
        ,cust_survey.gss_score
        ,cust_survey.generic_visitor_ratings
        ,cust_survey.park_visitor_ratings
        ,cust_survey.positive_comments
        ,cust_survey.negative_comments
        ,cust_survey.source_name

    FROM {catalog}.{schema}.customer_survey cust_survey
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON cust_survey.entity_identifiers.scv_sk = scv_mst_1.scv_account_sk
    LEFT JOIN {catalog}.{schema}.omni_account_master omn_mst_1
        ON cust_survey.entity_identifiers.omni_sk = omn_mst_1.omni_account_sk
    """,

    # ============================================================
    # vw_marketing
    # ============================================================
    "vw_marketing": """
    SELECT
        mktg.entity_identifiers.email_id AS email_entity_identifier

        -- SCV details
        ,IFF(mktg.entity_identifiers.scv_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details
        ,IFF(mktg.entity_identifiers.omni_sk IS NOT NULL, MAP(
            'omni_account_sk',        CAST(omn_mst_1.omni_account_sk AS STRING)
            ,'account_id',            CAST(omn_mst_1.account_id AS STRING)
            ,'email_id',              CAST(omn_mst_1.email_id AS STRING)
            ,'first_name',            CAST(omn_mst_1.first_name AS STRING)
            ,'last_name',             CAST(omn_mst_1.last_name AS STRING)
            ,'dob',                   CAST(omn_mst_1.dob AS STRING)
            ,'gender',                CAST(omn_mst_1.gender AS STRING)
            ,'mobile_country_code',   CAST(omn_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(omn_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(omn_mst_1.city AS STRING)
            ,'nationality_name',      CAST(omn_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(omn_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(omn_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(omn_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(omn_mst_1.pod_flag AS STRING)
            ,'parent_omni_account_sk', CAST(omn_mst_1.parent_omni_account_sk AS STRING)
            ,'parent_account_id',     CAST(omn_mst_1.parent_account_id AS STRING)
        ), NULL) AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'owner_scv_sk',  CAST(mktg.owner_scv_sk AS STRING)
            ,'owner_omni_sk', CAST(mktg.owner_omni_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,mktg.fact_key
        ,mktg.customer_email
        ,mktg.event_type
        ,mktg.event_time
        ,mktg.event_sub_type
        ,mktg.campaign_source
        ,mktg.message_id
        ,mktg.hardware
        ,mktg.unsubscribe_flag
        ,mktg.account_name
        ,mktg.campaign_key
        ,mktg.campaign_name
        ,mktg.utm_tag
        ,mktg.campaign_type
        ,mktg.campaign_category
        ,mktg.campaign_launch_date
        ,mktg.others
        ,mktg.source_name

    FROM {catalog}.{schema}.marketing mktg
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON mktg.entity_identifiers.scv_sk = scv_mst_1.scv_account_sk
    LEFT JOIN {catalog}.{schema}.omni_account_master omn_mst_1
        ON mktg.entity_identifiers.omni_sk = omn_mst_1.omni_account_sk
    """,

    # ============================================================
    # vw_touchpoints
    # ============================================================
    "vw_touchpoints": """
    SELECT
        tp.entity_identifiers.email_id AS email_entity_identifier

        -- SCV details
        ,IFF(tp.entity_identifiers.scv_sk IS NOT NULL, MAP(
            'scv_account_sk',         CAST(scv_mst_1.scv_account_sk AS STRING)
            ,'mdm_id',                CAST(scv_mst_1.mdm_id AS STRING)
            ,'yas_id',                CAST(scv_mst_1.yas_id AS STRING)
            ,'email_id',              CAST(scv_mst_1.email_id AS STRING)
            ,'first_name',            CAST(scv_mst_1.first_name AS STRING)
            ,'last_name',             CAST(scv_mst_1.last_name AS STRING)
            ,'dob',                   CAST(scv_mst_1.dob AS STRING)
            ,'gender',                CAST(scv_mst_1.gender AS STRING)
            ,'role_to_parent',        CAST(scv_mst_1.role_to_parent AS STRING)
            ,'mobile_country_code',   CAST(scv_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(scv_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(scv_mst_1.city AS STRING)
            ,'nationality_name',      CAST(scv_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(scv_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(scv_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(scv_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(scv_mst_1.pod_flag AS STRING)
            ,'parent_scv_account_sk', CAST(scv_mst_1.parent_scv_account_sk AS STRING)
            ,'parent_yas_id',         CAST(scv_mst_1.parent_yas_id AS STRING)
        ), NULL) AS scv_entity_identifier

        -- OMNI details
        ,IFF(tp.entity_identifiers.omni_sk IS NOT NULL, MAP(
            'omni_account_sk',        CAST(omn_mst_1.omni_account_sk AS STRING)
            ,'account_id',            CAST(omn_mst_1.account_id AS STRING)
            ,'email_id',              CAST(omn_mst_1.email_id AS STRING)
            ,'first_name',            CAST(omn_mst_1.first_name AS STRING)
            ,'last_name',             CAST(omn_mst_1.last_name AS STRING)
            ,'dob',                   CAST(omn_mst_1.dob AS STRING)
            ,'gender',                CAST(omn_mst_1.gender AS STRING)
            ,'mobile_country_code',   CAST(omn_mst_1.mobile_country_code AS STRING)
            ,'mobile_num',            CAST(omn_mst_1.mobile_num AS STRING)
            ,'city',                  CAST(omn_mst_1.city AS STRING)
            ,'nationality_name',      CAST(omn_mst_1.nationality_name AS STRING)
            ,'country_of_residence_name', CAST(omn_mst_1.country_of_residence_name AS STRING)
            ,'staff_flag',            CAST(omn_mst_1.staff_flag AS STRING)
            ,'dependent_flag',        CAST(omn_mst_1.dependent_flag AS STRING)
            ,'pod_flag',              CAST(omn_mst_1.pod_flag AS STRING)
            ,'parent_omni_account_sk', CAST(omn_mst_1.parent_omni_account_sk AS STRING)
            ,'parent_account_id',     CAST(omn_mst_1.parent_account_id AS STRING)
        ), NULL) AS omni_entity_identifier

        -- Entity SK list
        ,MAP(
            'owner_scv_sk',  CAST(tp.owner_scv_sk AS STRING)
            ,'owner_omni_sk', CAST(tp.owner_omni_sk AS STRING)
        ) AS entity_sk_list

        -- Domain columns
        ,tp.contact_profile
        ,tp.type
        ,tp.conversation_id
        ,tp.conversation_start
        ,tp.conversation_end
        ,tp.resolution_time
        ,tp.originating_direction
        ,tp.inquiry_type
        ,tp.affected_asset
        ,tp.ivr_details
        ,tp.payment_details
        ,tp.flow_details
        ,tp.inbound_communication_count
        ,tp.outbound_communication_count
        ,tp.total_conversation_participants
        ,tp.acd_queues_involved
        ,tp.agents_involved
        ,tp.workflows_involved
        ,tp.ivr_involved
        ,tp.voicemails_involved
        ,tp.others
        ,tp.source_name

    FROM {catalog}.{schema}.touchpoints tp
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON tp.entity_identifiers.scv_sk = scv_mst_1.scv_account_sk
    LEFT JOIN {catalog}.{schema}.omni_account_master omn_mst_1
        ON tp.entity_identifiers.omni_sk = omn_mst_1.omni_account_sk
    """,
    # ============================================================
    # vw_customer_kpi_by_scv_sk
    # ============================================================
    "vw_customer_kpi_by_scv_sk":"""
    WITH kpi_mapped AS (
        SELECT
            cus_kpi.report_date
            ,cus_kpi.report_hour
            ,cus_kpi.identifier_type
            ,cus_kpi.identifier_value
            ,mst_kpi.kpi_name
            ,cus_kpi.kpi_value
            ,cus_kpi.created_at
            ,cus_kpi.modified_at
        FROM {catalog}.{schema}.customer_kpi cus_kpi
        INNER JOIN {catalog}.{schema}.master_kpi mst_kpi
            ON cus_kpi.kpi_id = mst_kpi.kpi_id
        WHERE 1 = 1
            AND identifier_type = 'scv_sk'
            AND report_date = (
                SELECT MAX(report_date)
                FROM {catalog}.{schema}.customer_kpi
                WHERE identifier_type = 'scv_sk'
            )
        ORDER BY mst_kpi.kpi_name
    )
    ,kpi_pivoted AS (
        SELECT
            report_date
            ,report_hour
            ,identifier_type
            ,identifier_value
            ,map_from_entries(Array_sort(Collect_list(Struct(kpi_name, kpi_value)))) AS category_json
            ,created_at
            ,modified_at
        FROM kpi_mapped
        GROUP BY
            report_date
            ,report_hour
            ,identifier_type
            ,identifier_value
            ,created_at
            ,modified_at
    )
    SELECT
        scv_master.scv_account_sk
        ,scv_master.parent_scv_account_sk
        ,IFF(report_date IS NULL, 'N', 'Y') AS kpi_flag
        ,scv_master.email_id
        ,scv_master.yas_id
        ,scv_master.first_name
        ,scv_master.last_name
        ,scv_master.dob
        ,scv_master.gender
        ,scv_master.LANGUAGE
        ,scv_master.role_to_parent
        ,scv_master.mobile_country_code
        ,scv_master.mobile_num
        ,scv_master.city
        ,scv_master.country_of_residence_name
        ,scv_master.nationality_name
        ,scv_master.staff_flag
        ,scv_master.pod_flag
        ,scv_master.dependent_flag
        ,report_date
        ,report_hour
        ,category_json AS kpi_json
        ,created_at
        ,modified_at
    FROM {catalog}.{schema}.scv_account_master scv_master
    LEFT JOIN kpi_pivoted
        ON kpi_pivoted.identifier_value = scv_master.scv_account_sk
    """,
    # ============================================================
    # tbvw_customer_kpi_by_omni_sk
    # ============================================================
    "tbvw_customer_kpi_by_omni_sk":"""
    WITH kpi_mapped AS (
        SELECT
            cus_kpi.report_date
            ,cus_kpi.report_hour
            ,cus_kpi.identifier_type
            ,cus_kpi.identifier_value
            ,mst_kpi.kpi_name
            ,cus_kpi.kpi_value
            ,cus_kpi.created_at
            ,cus_kpi.modified_at
        FROM {catalog}.{schema}.customer_kpi cus_kpi
        INNER JOIN {catalog}.{schema}.master_kpi mst_kpi
            ON cus_kpi.kpi_id = mst_kpi.kpi_id
        WHERE 1 = 1
            AND identifier_type = 'omni_sk'
            AND report_date = (
                SELECT MAX(report_date)
                FROM {catalog}.{schema}.customer_kpi
                WHERE identifier_type = 'omni_sk'
            )
        ORDER BY mst_kpi.kpi_name
    )
    ,kpi_pivoted AS (
        SELECT
            report_date
            ,report_hour
            ,identifier_type
            ,identifier_value
            ,map_from_entries(Array_sort(Collect_list(Struct(kpi_name, kpi_value)))) AS category_json
            ,created_at
            ,modified_at
        FROM kpi_mapped
        GROUP BY
            report_date
            ,report_hour
            ,identifier_type
            ,identifier_value
            ,created_at
            ,modified_at
    )
    SELECT
        omni_master.omni_account_sk
        ,omni_master.parent_omni_account_sk
        ,IFF(report_date IS NULL, 'N', 'Y') AS kpi_flag
        ,omni_master.email_id
        ,omni_master.account_id
        ,omni_master.first_name
        ,omni_master.last_name
        ,omni_master.dob
        ,omni_master.gender
        ,omni_master.mobile_country_code
        ,omni_master.mobile_num
        ,omni_master.city
        ,omni_master.country_of_residence_name
        ,omni_master.nationality_name
        ,omni_master.staff_flag
        ,omni_master.pod_flag
        ,omni_master.dependent_flag
        ,report_date
        ,report_hour
        ,category_json AS kpi_json
        ,created_at
        ,modified_at
    FROM {catalog}.{schema}.omni_account_master omni_master
    LEFT JOIN kpi_pivoted
        ON kpi_pivoted.identifier_value = omni_master.omni_account_sk
    """,
    # ============================================================
    # tbvw_customer_kpi_by_email
    # ============================================================
    "tbvw_customer_kpi_by_email":"""
    WITH kpi_mapped AS (
        SELECT
            cus_kpi.report_date
            ,cus_kpi.report_hour
            ,cus_kpi.identifier_type
            ,cus_kpi.identifier_value
            ,mst_kpi.kpi_name
            ,cus_kpi.kpi_value
            ,cus_kpi.created_at
            ,cus_kpi.modified_at
        FROM {catalog}.{schema}.customer_kpi cus_kpi
        INNER JOIN {catalog}.{schema}.master_kpi mst_kpi
            ON cus_kpi.kpi_id = mst_kpi.kpi_id
        WHERE 1 = 1
            AND identifier_type = 'email_id'
            AND report_date = (
                SELECT MAX(report_date)
                FROM {catalog}.{schema}.customer_kpi
                WHERE identifier_type = 'email_id'
            )
        ORDER BY mst_kpi.kpi_name
    )
    ,kpi_pivoted AS (
        SELECT
            report_date
            ,report_hour
            ,identifier_type
            ,identifier_value
            ,map_from_entries(Array_sort(Collect_list(Struct(kpi_name, kpi_value)))) AS category_json
            ,created_at
            ,modified_at
        FROM kpi_mapped
        GROUP BY
            report_date
            ,report_hour
            ,identifier_type
            ,identifier_value
            ,created_at
            ,modified_at
    )
    SELECT
        scv_master.email_id
        ,IFF(report_date IS NULL, 'N', 'Y') AS kpi_flag
        ,scv_master.mdm_id
        ,scv_master.yas_id
        ,scv_master.first_name
        ,scv_master.last_name
        ,scv_master.dob
        ,scv_master.gender
        ,scv_master.mobile_country_code
        ,scv_master.mobile_num
        ,scv_master.city
        ,scv_master.country_of_residence_name
        ,scv_master.nationality_name
        ,scv_master.staff_flag
        ,scv_master.pod_flag
        ,scv_master.dependent_flag
        ,report_date
        ,report_hour
        ,category_json AS kpi_json
        ,created_at
        ,modified_at
    FROM {catalog}.{schema}.scv_account_master scv_master
    LEFT JOIN kpi_pivoted
        ON kpi_pivoted.identifier_value = scv_master.email_id
    WHERE scv_master.source_name = 't_mdm_guest'
    """,
    # ============================================================
    # tbvw_customer_kpi_by_email_o2o
    # ============================================================
    "tbvw_customer_kpi_by_email_o2o":"""
    WITH kpi_mapped AS (
        SELECT
            cus_kpi.report_date
            ,cus_kpi.report_hour
            ,cus_kpi.identifier_type
            ,cus_kpi.identifier_value
            ,mst_kpi.kpi_name
            ,cus_kpi.kpi_value
            ,cus_kpi.created_at
            ,cus_kpi.modified_at
        FROM {catalog}.{schema}.customer_kpi cus_kpi
        INNER JOIN {catalog}.{schema}.master_kpi mst_kpi
            ON cus_kpi.kpi_id = mst_kpi.kpi_id
        WHERE 1 = 1
            AND identifier_type = 'email_id'
            AND report_date = (
                SELECT MAX(report_date)
                FROM {catalog}.{schema}.customer_kpi
                WHERE identifier_type = 'email_id'
            )
        ORDER BY mst_kpi.kpi_name
    )
    ,kpi_pivoted AS (
        SELECT
            report_date
            ,report_hour
            ,identifier_type
            ,identifier_value
            ,map_from_entries(Array_sort(Collect_list(Struct(kpi_name, kpi_value)))) AS category_json
            ,created_at
            ,modified_at
        FROM kpi_mapped
        GROUP BY
            report_date
            ,report_hour
            ,identifier_type
            ,identifier_value
            ,created_at
            ,modified_at
    )
    SELECT
        sha2(scv_master.email_id, 256) as email_id_hash
        ,scv_master.mdm_id as mdm_id
        ,scv_master.yas_id as yas_id
        ,sha2(scv_master.first_name, 256) as first_name_hash
        ,sha2(scv_master.last_name, 256) as last_name_hash
        ,sha2(cast(floor(DATEDIFF(current_date(), scv_master.dob) / 365) as STRING), 256) as age_hash
        ,sha2(scv_master.gender, 256) as gender_hash
        ,sha2(scv_master.country_of_residence_name, 256) as country_of_residence_hash
        ,sha2(scv_master.city, 256) as city_hash
        ,sha2(cast(scv_master.dob as STRING), 256) as dob_hash
        ,sha2(scv_master.nationality_name, 256) as nationality_hash
        ,scv_master.language
        ,scv_master.guest_type
        ,scv_master.staff_flag
        ,coalesce(round(category_json['NO_PARK_VISITS_1MTHS']), 0) AS NO_PARK_VISITS_1MTHS
        ,coalesce(round(category_json['NO_HOTEL_STAYS_1MTHS']), 0) AS NO_HOTEL_STAYS_1MTHS
        ,coalesce(round(category_json['AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_1MTHS']), 0) AS AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_1MTHS
        ,coalesce(round(category_json['NO_PARK_VISITS_3MTHS']), 0) AS NO_PARK_VISITS_3MTHS
        ,coalesce(round(category_json['NO_HOTEL_STAYS_3MTHS']), 0) AS NO_HOTEL_STAYS_3MTHS
        ,coalesce(round(category_json['AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_3MTHS']), 0) AS AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_3MTHS
        ,coalesce(round(category_json['NO_PARK_VISITS_6MTHS']), 0) AS NO_PARK_VISITS_6MTHS
        ,coalesce(round(category_json['NO_HOTEL_STAYS_6MTHS']), 0) AS NO_HOTEL_STAYS_6MTHS
        ,coalesce(round(category_json['AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_6MTHS']), 0) AS AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_6MTHS
        ,coalesce(round(category_json['NO_PARK_VISITS_12MTHS']), 0) AS NO_PARK_VISITS_12MTHS
        ,coalesce(round(category_json['NO_HOTEL_STAYS_12MTHS']), 0) AS NO_HOTEL_STAYS_12MTHS
        ,coalesce(round(category_json['AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_12MTHS']), 0) AS AVG_LENGTH_OF_STAY_YAS_IN_SECONDS_12MTHS
        ,category_json['LAST_CHECKIN_TIMESTAMP'] AS LAST_CHECKIN_TIMESTAMP
        ,category_json['LATEST_PARK_VISITED'] AS LATEST_PARK_VISITED
        ,category_json['FIRST_CHECKIN_TIMESTAMP'] AS FIRST_CHECKIN_TIMESTAMP
        ,category_json['FIRST_PARK_VISITED'] AS FIRST_PARK_VISITED
        ,category_json['FAVORIT_PARK'] AS FAVORIT_PARK
        ,coalesce(round(category_json['TOTAL_REVENUE_GENERATED_LAST_1_MONTHS']), 0) AS TOTAL_REVENUE_GENERATED_LAST_1_MONTHS
        ,coalesce(round(category_json['TOTAL_NUMBER_VISITS_LAST_1_MONTHS']), 0) AS TOTAL_NUMBER_VISITS_LAST_1_MONTHS
        ,coalesce(round(category_json['AVG_SPENT_VISIT_LAST_1_MONTHS']), 0) AS AVG_SPENT_VISIT_LAST_1_MONTHS
        ,coalesce(round(category_json['TOTAL_REVENUE_GENERATED_LAST_3_MONTHS']), 0) AS TOTAL_REVENUE_GENERATED_LAST_3_MONTHS
        ,coalesce(round(category_json['TOTAL_NUMBER_VISITS_LAST_3_MONTHS']), 0) AS TOTAL_NUMBER_VISITS_LAST_3_MONTHS
        ,coalesce(round(category_json['AVG_SPENT_VISIT_LAST_3_MONTHS']), 0) AS AVG_SPENT_VISIT_LAST_3_MONTHS
        ,coalesce(round(category_json['TOTAL_REVENUE_GENERATED_LAST_6_MONTHS']), 0) AS TOTAL_REVENUE_GENERATED_LAST_6_MONTHS
        ,coalesce(round(category_json['TOTAL_NUMBER_VISITS_LAST_6_MONTHS']), 0) AS TOTAL_NUMBER_VISITS_LAST_6_MONTHS
        ,coalesce(round(category_json['AVG_SPENT_VISIT_LAST_6_MONTHS']), 0) AS AVG_SPENT_VISIT_LAST_6_MONTHS
        ,coalesce(round(category_json['TOTAL_REVENUE_GENERATED_LAST_12_MONTHS']), 0) AS TOTAL_REVENUE_GENERATED_LAST_12_MONTHS
        ,coalesce(round(category_json['TOTAL_NUMBER_VISITS_LAST_12_MONTHS']), 0) AS TOTAL_NUMBER_VISITS_LAST_12_MONTHS
        ,coalesce(round(category_json['AVG_SPENT_VISIT_LAST_12_MONTHS']), 0) AS AVG_SPENT_VISIT_LAST_12_MONTHS
        -- ,category_json['CHURN_STATUS_REALIZATION_DATE'] AS CHURN_STATUS_REALIZATION_DATE
        -- ,category_json['CHURN_STATUS'] AS CHURN_STATUS
        ,round(category_json['DAYS_SINCE_LAST_VISIT']) AS DAYS_SINCE_LAST_VISIT
        ,round(category_json['IS_TODAY_LESS_90_DAYS']) AS IS_TODAY_LESS_90_DAYS
        ,round(category_json['IS_YESTERDAY_LESS_90_DAYS']) AS IS_YESTERDAY_LESS_90_DAY
        ,(select max(report_date) from kpi_pivoted) as last_updated_date
    FROM {catalog}.{schema}.scv_account_master scv_master
    LEFT JOIN kpi_pivoted
        ON kpi_pivoted.identifier_value = scv_master.email_id
    WHERE scv_master.source_name = 't_mdm_guest'
    """,
    # ============================================================
    # vw_location_intelligence_o2o
    # ============================================================
    "vw_location_intelligence_o2o":"""
    SELECT
    sha2(entity_identifiers.email_id, 256) as email_id_hash
    ,scv_mst_1.mdm_id as mdm_id
    ,location_intelligence.asset_name
    ,location_intelligence.checkin_timestamp
    ,location_intelligence.product_name
    ,location_intelligence.annual_pass_flag
    ,location_intelligence.others.ticket_id as ticket_id
    ,location_intelligence.etl_updated_datetime
    FROM {catalog}.{schema}.location_intelligence
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON entity_identifiers.email_id = scv_mst_1.email_id
        AND scv_mst_1.source_name = 't_mdm_guest'
    WHERE 
    location_intelligence.category = 'ATTENDANCE'
    and scv_mst_1.mdm_id is not null
    """,
    # ============================================================
    # vw_transactions_o2o
    # ============================================================
    "vw_transactions_o2o":"""
    SELECT
    sha2(transactions.entity_identifiers.email_id, 256) as email_id_hash
    ,scv_mst_1.mdm_id as mdm_id
    ,transactions.sale_timestamp
    ,transactions.sale_id
    ,transactions.ticket_id
    ,transactions.channel
    ,transactions.channel_area
    ,transactions.product_category
    ,transactions.product_name
    ,transactions.product_subcategory
    ,transactions.promotion_name
    ,transactions.gross_sale_amount
    ,transactions.quantity
    ,transactions.etl_updated_datetime
    FROM {catalog}.{schema}.transactions transactions
    LEFT JOIN {catalog}.{schema}.scv_account_master scv_mst_1
        ON transactions.entity_identifiers.email_id = scv_mst_1.email_id
        AND scv_mst_1.source_name = 't_mdm_guest'
    WHERE transactions.sale_source_system = 'OMNI'
        and transactions.product_category = 'Ticket'
        and scv_mst_1.mdm_id is not null
        and transactions.quantity > 0
    """
}
