{%- macro get_ga4_events_insights__child_source(table_name) -%}

{%- set event_types = dbt_utils.get_column_values(source('ga4_raw',table_name),'event_name') -%}

SELECT 
    date,
    SPLIT_PART(property,'/',2) as profile,
    first_user_source_medium as source_medium,
    first_user_campaign_name as campaign_name,
    first_user_campaign_id as campaign_id,
    {%- if 'granular' in table_name %}
        first_user_manual_ad_content as ad,
        first_user_manual_term as term,
        landing_page,
    {% endif -%}
    {% for event_type in event_types -%}
        COALESCE(SUM(CASE WHEN event_name = '{{event_type}}' THEN event_count ELSE 0 END), 0) as "{{event_type}}"
    {%- if not loop.last %},{% endif -%}
    {%- endfor -%}

FROM {{ source('ga4_raw',table_name) }}
    
{%- if 'granular' in table_name %}
GROUP BY 1,2,3,4,5,6,7,8
{%- else %}
GROUP BY 1,2,3,4,5
{% endif -%}

{%- endmacro %}
