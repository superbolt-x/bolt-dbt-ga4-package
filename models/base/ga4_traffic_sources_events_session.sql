WITH event_table AS 
    ({{ get_ga4_events_insights__child_source('events_session') }})
  
SELECT *,
    {{ get_date_parts('date') }},
    date||'_'||profile||'_'||source_medium||'_'||campaign_name||'_'||campaign_id as unique_key
FROM event_table 
