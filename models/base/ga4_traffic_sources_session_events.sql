WITH event_table AS 
    ({{ get_ga4_events_insights__child_source('events_session') }})
  
SELECT *,
    {{ get_date_parts('date') }}
FROM event_table 
