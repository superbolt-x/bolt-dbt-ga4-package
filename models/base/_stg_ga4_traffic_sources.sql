{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'google_analytics_4', 'traffic_sources' -%}

{%- set exclude_fields = [
   "_fivetran_id"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  
{%- set primary_keys = ['date','profile','source_medium','campaign','google_ads_keyword','manual_ad_content','landing_page'] -%} -- is ga keyword necessary ?

WITH raw_table AS 
    (SELECT 
        {%- for field in fields %}
        {{ get_ga4_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, table_name) }}
    {% if is_incremental() -%}

    -- this filter will only be applied on an incremental run
    where date >= (select max(date) from {{ this }})

    {% endif %}
    ),

    event_table AS (
            {{ get_facebook_ads_insights__child_source('event') }}
    ),

    staging AS 
    (SELECT *,
        sessions * average_session_duration as session_duration,
        sessions - engaged_sessions as bounced_sessions--,
        --sessions * percent_new_sessions/100 as new_sessions
    FROM raw_table
    )
/*
    {%- set goal_table_exists = check_source_exists(schema_name, 'traffic_sources_goals') -%}
    {%- if goal_table_exists %}

    , goals AS 
    (SELECT {{ primary_keys|join(', ') }},
        {% for n in range(1,11) -%}
        goal_{{n}}_completions
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ source(schema_name, 'traffic_sources_goals') }}
    )
    {%- endif %}

    {%- set goal_bis_table_exists = check_source_exists(schema_name, 'traffic_sources_goals_bis') %}
    {%- if goal_bis_table_exists %}
    , goals_bis AS 
    (SELECT 
        {{ primary_keys|join(', ') }},
        {% for n in range(11,21) -%}
        goal_{{n}}_completions
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ source(schema_name, 'traffic_sources_goals_bis') }}
    )
    {%- endif %}
*/

SELECT *,
    MAX(_fivetran_synced) over () as last_updated,
    date||'_'||profile||'_'||source_medium||'_'||campaign||'_'||google_ads_keyword||'_'||manual_ad_content||'_'||landing_page as unique_key
FROM staging
LEFT JOIN event_table USING(date,campaign_name)
/*
{%- set goal_table_exists = check_source_exists(schema_name, 'traffic_sources_goals') %}
{%- if goal_table_exists %}
LEFT JOIN goals USING({{ primary_keys|join(',') }})
{%- endif %}
{%- set goal_bis_table_exists = check_source_exists(schema_name, 'traffic_sources_goals_bis') %}
{%- if goal_bis_table_exists %}
LEFT JOIN goals_bis USING({{ primary_keys|join(',') }})
{%- endif %}
*/
