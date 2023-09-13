{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'ga4_raw', 'location' -%}

{%- set exclude_fields = [
   "_fivetran_id"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  
{%- set primary_keys = ['date','profile','continent','country','region','city'] -%} -- is ga keyword necessary ?

WITH raw_table AS 
    (SELECT 
        {%- for field in fields %}
        {{ get_ga4_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, table_name) }}
    {%- if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where date >= (select max(date) from {{ this }})

    {% endif -%}

    ),

    event_table AS (
            {{ get_ga4_events_insights__child_source('location_events') }}
    ),

    staging AS 
    (SELECT *,
        sessions * average_session_duration as session_duration,
        sessions - engaged_sessions as bounced_sessions--,
        --sessions * percent_new_sessions/100 as new_sessions
    FROM raw_table
    )

SELECT *,
    MAX(_fivetran_synced) over () as last_updated,
    date||'_'||profile||'_'||continent||'_'||country||'_'||region||'_'||city|| as unique_key
FROM staging
LEFT JOIN event_table USING(date,profile,continent,country,region,city)
