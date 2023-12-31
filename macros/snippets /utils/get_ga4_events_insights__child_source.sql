{%- macro get_ga4_events_insights__child_source(table_name) -%}
    
{%- if 'session' in table_name %}
    {%- set event_types = dbt_utils.get_column_values(ref('_stg_ga4_events_session'),'event_name',order_by='event_name') -%}
    {%- set rank = dbt_utils.get_column_values(ref('_stg_ga4_events_session'),'rank',order_by='rank') -%}
    {%- set dup_events = dbt_utils.get_column_values(ref('_stg_ga4_events_session'),'event_name',where='dup_event_name_nb > 1',default=[]) -%}
{%- else -%}
    {%- set event_types = dbt_utils.get_column_values(ref('_stg_ga4_events'),'event_name',order_by='event_name') -%}
    {%- set rank = dbt_utils.get_column_values(ref('_stg_ga4_events'),'rank',order_by='rank') -%}
    {%- set dup_events = dbt_utils.get_column_values(ref('_stg_ga4_events'),'event_name',where='dup_event_name_nb > 1',default=[]) -%}
{% endif -%}

SELECT 
    date,
    SPLIT_PART(property,'/',2) as profile,
    
    {%- if 'location' not in table_name and 'session' not in table_name %}
        first_user_source_medium as source_medium,
        first_user_campaign_name as campaign_name,
        first_user_campaign_id as campaign_id,

    {%- elif 'session' in table_name %}
        session_source_medium as source_medium,
        session_campaign_name as campaign_name,
        session_campaign_id as campaign_id,
    
    {%- else -%}
        city,
        region,
        country,
        continent,
    {% endif -%}
    
    {%- if 'granular' in table_name %}
        first_user_manual_ad_content as ad,
        landing_page,
    {% endif -%}
    
    {%- for event_type, event_type_nb in zip(event_types,rank) -%}
        {%- if '"' not in event_type and '\u0027' not in event_type %}
            {%- if event_type in dup_events %}
            COALESCE(SUM(CASE WHEN event_name = '{{event_type}}' THEN event_count ELSE 0 END), 0) as {{ adapter.quote(event_type~'_'~event_type_nb) }}
            {%- else -%}
            COALESCE(SUM(CASE WHEN event_name = '{{event_type}}' THEN event_count ELSE 0 END), 0) as {{ adapter.quote(event_type) }}
            {% endif -%}
            {%- if not loop.last %},{% endif -%}
        {% endif -%}
    {%- endfor -%}

FROM {{ source('ga4_raw',table_name) }}

GROUP BY 
    date,
    profile,
{%- if 'location' not in table_name %}
    source_medium,
    campaign_name,
    campaign_id
{%- else -%}
    city,
    region,
    country,
    continent
{% endif -%}
{%- if 'granular' in table_name %}
    ,ad,
    landing_page
{% endif -%}
    
{%- endmacro %}
