{{ config (
    alias = target.database + '_ga4_performance_by_city'
)}}
    
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set reject_list = ['date','profile','country','day','week','month','quarter','year','region','city','_fivetran_synced','unique_key','last_updated'] -%}
{%- set fields = adapter.get_columns_in_relation(ref('ga4_locations'))
                    |map(attribute="name")
                    |reject("in",reject_list)
                    |list
                    -%}  

WITH 
    {% for date_granularity in date_granularity_list -%}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        profile,
        country,
        region,
        city,
        {%- for field in fields %}
        COALESCE(SUM("{{ field }}"),0) as "{{ field }}" 
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        
    FROM {{ ref('ga4_locations') }}
    GROUP BY 1,2,3,4,5,6)

    {%- if not loop.last %},

    {% endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT * 
FROM performance_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %}
