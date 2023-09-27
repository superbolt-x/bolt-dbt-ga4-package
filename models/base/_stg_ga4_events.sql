{%- set event_types = dbt_utils.get_column_values(source('ga4_raw','events'),'event_name') -%}

   WITH events_lower AS (
      SELECT
        DISTINCT(event_name) AS event
      FROM {{ source('ga4_raw','events') }}
  )

SELECT LOWER(event) as event_name, count(*) as nb FROM events_lower GROUP BY event_name having nb > 1
