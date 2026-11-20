SELECT 
	name,
	status,
	source,
	title[OFFSET(0)].k as title_k,
	title[OFFSET(0)].v as title_v,
	message[OFFSET(0)].k as message_k,
	message[OFFSET(0)].v as message_v,
	application_id,
	android_settings,
	ios_settings,
	segment_id,
	data,
	target,
	launched_at,
	scheduled_at,
	created_at,
	deleted_at,
	customer_id,
	push_internal_campaign_id,
	campaign_id,
	partitiontime,
	event_time,
	loaded_at,
	TO_HEX(SHA256(
    CONCAT(
      COALESCE(CAST(campaign_id AS STRING), ''), '|',
	  COALESCE(CAST(customer_id AS STRING), ''), '|',
      COALESCE(CAST(application_id AS STRING), ''), '|',
      COALESCE(CAST(segment_id AS STRING), ''), '|',
      COALESCE(CAST(event_time AS STRING), '')
    )
  )) AS fact_key
FROM emarsys_miralbtoc_876026434.push_campaigns_876026434
