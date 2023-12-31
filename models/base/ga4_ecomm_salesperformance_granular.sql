{%- set currency_fields = [
    "transaction_revenue"
]
-%}

{%- set exclude_fields = [
    "_fivetran_synced"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_ga4_ecomm_salesperformance_granular'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH 
    {% if var('currency') != 'USD' -%}
        currency AS (
            SELECT DISTINCT date, "{{ var('currency') }}" as raw_rate, 
            LAG(raw_rate) ignore nulls over (order by date) as exchange_rate
            FROM utilities.dates 
            LEFT JOIN utilities.currency USING(date)
            WHERE date <= current_date),
    {%- endif -%}

    {%- set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}
    
    insights AS 
    (SELECT 
        {%- for field in stg_fields -%}
            {%- if field in currency_fields %}
                "{{ field }}"::float/{{ exchange_rate }} as "{{ field }}"
            {%- else %}
                "{{ field }}"
            {%- endif -%}
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    
    FROM {{ ref('_stg_ga4_ecomm_salesperformance_granular') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    )

SELECT *,
    {{ get_date_parts('date') }}
FROM insights 
