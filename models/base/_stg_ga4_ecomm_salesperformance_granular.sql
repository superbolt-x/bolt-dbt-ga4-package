{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'ga4_raw', 'granular_ecomm_salesperformance' -%}   

{%- set exclude_fields = [
   "_fivetran_id"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |list
                    -%}  

WITH staging AS 
    (SELECT date, 
            SPLIT_PART(property,'/',2) as profile, 
            first_user_source_medium as source_medium, 
            first_user_campaign_name as campaign_name,
            first_user_campaign_id as campaign_id,
            first_user_manual_ad_content as ad,
            landing_page,
            transaction_id,
            ecommerce_purchases as transactions,
            total_revenue as transaction_revenue
    
     FROM {{ source(schema_name, table_name) }}
        
     {% if is_incremental() -%}

     -- this filter will only be applied on an incremental run
     where date >= (select max(date)-1 from {{ this }})

    {% endif %}
        
    )

SELECT *,
    date||'_'||profile||'_'||source_medium||'_'||campaign_name||'_'||campaign_id||'_'||ad||'_'||landing_page||'_'||transaction_id as unique_key
FROM staging
