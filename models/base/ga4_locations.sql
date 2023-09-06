{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'ga4_raw', 'location' -%}

{%- set exclude_fields = [
   "_fivetran_id",
   "continent"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH raw_table AS 
    (SELECT 
        {%- for field in fields %}
        {{ get_googleanalytics_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, table_name) }}
    ),

    staging AS 
    (SELECT *,
        sessions * average_session_duration as session_duration,
        sessions - engaged_sessions as bounced_sessions
    FROM raw_table
    )

SELECT *,
    {{ get_date_parts('date') }},
    MAX(_fivetran_synced) over () as last_updated,
    date||'_'||profile||'_'||country||'_'||region||'_'||city as unique_key
FROM staging
{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date) from {{ this }})

{% endif %}
