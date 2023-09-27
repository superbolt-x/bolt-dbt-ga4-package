{{ config( 
        materialized='incremental',
        unique_key='event_name'
) }}

{%- set event_types = dbt_utils.get_column_values(source('ga4_raw','events'),'event_name') -%}
    
   WITH distinct_events AS (
    SELECT DISTINCT(event_name) AS event_name
    FROM {{ source('ga4_raw','events') }}
     ),
   
   dup_events AS (
    SELECT LOWER(event_name) as lower_event_name, count(*) as nb 
    FROM distinct_events 
    GROUP BY lower_event_name 
    HAVING nb > 1
   )
   
   SELECT 
    event_name, 
    coalesce(lower_event_name,event_name) as dup_event_name,
    ROW_NUMBER() OVER (PARTITION BY dup_event_name) as dup_event_name_nb
   FROM distinct_events 
   LEFT JOIN dup_events 
   ON LOWER(distinct_events.event_name) = dup_events.lower_event_name 

