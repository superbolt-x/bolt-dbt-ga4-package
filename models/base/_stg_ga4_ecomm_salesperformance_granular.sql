{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'ga4_raw', 'granular_ecomm_salesperformance' -%}   --import raw table

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
            content_id,
            content_type as content,
            first_user_google_ads_keyword as keyword,
            landing_page as page_path,
            transaction_id,
            --MAX(sessions_to_transaction) as sessions_to_transaction,
            --MAX(days_to_transaction) as days_to_transaction,
            AVG(items_purchased) as item_quantity,
            AVG(item_revenue) as transaction_revenue
    
    FROM raw_table
    GROUP BY date, profile, source_medium, campaign_id, transaction_id, content, content_type, content_id, keyword, page_path
    )

SELECT *,
    date||'_'||profile||'_'||source_medium||'_'||campaign_id||'_'||transaction_id as unique_key
FROM staging
