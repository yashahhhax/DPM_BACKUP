SELECT
	contact_id,
	campaign_id,
	push_token,
	platform,
	program_id,
	reason,
	application_id,
	application_code,
	hardware_id,
	customer_id,
	source.id as source_id,
	source.type as source_type,
	partitiontime,
	event_time,
	loaded_at,
	treatments,
	TO_HEX(SHA256(
    CONCAT(
      COALESCE(CAST(campaign_id AS STRING), ''), '|',
      COALESCE(CAST(contact_id AS STRING), ''), '|',
	  COALESCE(CAST(customer_id AS STRING), ''), '|',
      COALESCE(CAST(hardware_id AS STRING), ''), '|',
      COALESCE(CAST(event_time AS STRING), '')
    )
  )) AS fact_key
FROM emarsys_miralbtoc_876026434.push_not_sends_876026434
