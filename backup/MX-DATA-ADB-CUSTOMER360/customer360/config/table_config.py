# Databricks notebook source
c360_table_config = {
      "transactions": {
      "target_table": "transformation_{env}.plt_customer360.transactions",
      "cluster_by": "sale_source_system,sale_timestamp,purchaser_scv_sk",
      },
      "location_intelligence":{
      "target_table": "transformation_{env}.plt_customer360.location_intelligence",
      "cluster_by": "category, checkin_timestamp, purchaser_scv_sk",
      },
      "customer_kpi": {
      "target_table": "transformation_{env}.plt_customer360.customer_kpi",
      "cluster_by": "identifier_type, report_date, identifier_value, kpi_id",
      },
      "marketing": {
      "target_table": "transformation_{env}.plt_customer360.marketing",
      "cluster_by": "owner_scv_sk, event_time",
      }
}
