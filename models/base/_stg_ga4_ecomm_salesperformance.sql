{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'ga4_raw', 'ecomm_salesperformance' -%}   --import raw table

{%- set exclude_fields = [
   "_fivetran_id"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

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

    staging AS 
    (SELECT date, 
            profile, 
            first_user_source_medium as source_medium, 
            first_user_campaign_name as campaign_name, 
            first_user_campaign_id as campaign_id, 
            transaction_id,
            ecommerce_purchase as transactions,
            total_revenue as transaction_revenue
    
    FROM raw_table
    )

SELECT *,
    date||'_'||profile||'_'||source_medium||'_'||campaign_name||'_'||campaign_id||'_'||transaction_id as unique_key
FROM staging
