{{ config (
    alias = target.database + '_ga4_performance_by_sourcemedium'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set dimensions_list = ['date','profile','source_medium'] -%}
{%- set fields = adapter.get_columns_in_relation(ref('ga4_traffic_sources'))
                    |map(attribute="name")
                    |reject("in",dimensions_list)
                    |list
                    -%}  

WITH 
    {% for date_granularity in date_granularity_list -%}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        profile,
        source_medium,
        {%- for field in fields %}
        COALESCE(SUM(field),0) as field,
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        
    FROM {{ ref('ga4_traffic_sources') }}
    GROUP BY 1,2,3,4)

    {%- if not loop.last %},

    {% endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT * 
FROM performance_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %}
