{%- macro get_ga4_events_insights__child_source(table_name) -%}

{%- set event_table = source('ga4_raw',table_name) -%}
{%- set event_types = dbt_utils.get_column_values(event_table,'event_name') -%}

SELECT 
    date,
    first_user_source_medium as source_medium,
    first_user_campaign_name as campaign_name,
    first_user_campaign_id as campaign_id,
    {%- if 'granular' in table_name %}
    first_user_manual_ad_content as ad,
    first_user_manual_term as term,
    landing_page,
    {% endif -%}
    {% for event_type in event_types -%}
        COALESCE(SUM(CASE WHEN event_type = '{{event_type}}' THEN event_count ELSE 0 END), 0) as {{event_type}},
        COALESCE(SUM(CASE WHEN event_type = '{{event_type}}' THEN event_value ELSE 0 END), 0) as "{{event_type}}_value"
    {%- if not loop.last %},{% endif -%}
    {%- endfor -%}

FROM event_table
GROUP BY 1,2

{%- endmacro %}
