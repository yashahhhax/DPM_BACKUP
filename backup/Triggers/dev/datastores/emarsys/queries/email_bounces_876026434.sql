Select *,
    TO_HEX(SHA256(
    CONCAT(
      COALESCE(CAST(campaign_id AS STRING), ''), '|',
      COALESCE(CAST(contact_id AS STRING), ''), '|',
      COALESCE(CAST(customer_id AS STRING), ''), '|',
      COALESCE(CAST(launch_id AS STRING), ''), '|',
      COALESCE(CAST(message_id AS STRING), ''), '|',
      COALESCE(CAST(event_time AS STRING), '')
    )
  )) AS fact_key
FROM emarsys_miralbtoc_876026434.email_bounces_876026434
