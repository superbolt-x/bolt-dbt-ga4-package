WITH event_table AS 
    ({{ get_ga4_events_insights__child_source('granular_events') }})
  
SELECT *,
    {{ get_date_parts('date') }},
    date||'_'||profile||'_'||source_medium||'_'||campaign_name||'_'||campaign_id||'_'||ad||'_'||landing_page as unique_key
FROM event_table 
