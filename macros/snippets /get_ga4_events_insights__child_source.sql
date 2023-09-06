{%- macro get_ga4_events_insights__child_source(table_name) -%}

{%- set event_table = source('ga4_raw','event_sources') -%}
{%- set event_types = dbt_utils.get_column_values(event_table,'event_name') -%}

SELECT 
    date,
    session_campaign_name as campaign_name,
    {% for event_type in event_types -%}
        COALESCE(SUM(CASE WHEN event_type = '{{event_type}}' THEN value ELSE 0 END), 0) as {{event_type}},
    {% endfor %}    

FROM event_table
GROUP BY 1,2

{%- endmacro %}
