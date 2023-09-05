{{ config (
    alias = target.database + '_ga4_performance_by_campaign'
)}}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    {% for date_granularity in date_granularity_list -%}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date, -- to keep ? 
        profile, --to keep? 
        source_medium,
        campaign_name,
        COALESCE(SUM(sessions),0) as sessions,
        --COALESCE(SUM(new_sessions),0) as new_sessions, to remplace by new user ?
        COALESCE(SUM(bounced_sessions),0) as bounced_sessions,
        COALESCE(SUM(session_duration),0) as session_duration,
        COALESCE(SUM(screen_page_views),0) as pageviews,
        COALESCE(SUM(transactions),0) as purchases,
        COALESCE(SUM(total_revenue),0) as revenue
        
    FROM {{ ref('ga4_traffic_sources') }}
    GROUP BY 1,2,3,4,5)

    {%- if not loop.last %},

    {% endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT * 
FROM performance_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %}