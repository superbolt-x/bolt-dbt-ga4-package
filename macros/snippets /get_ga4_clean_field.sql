{%- macro get_ga4_clean_field(table_name, column_name) %}


    {%- if table_name == 'pages' -%}
        {%- if column_name == 'page_path' -%}
        {{column_name}},
        SPLIT_PART({{column_name}},'?',1) as page

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}
    /*
    {%- else -%}
        {%- if column_name == 'property' -%}
        SPLIT_PART({{column_name}},'/',1) as profile,
        {%- elif column_name == 'first_user_source_medium' -%}
        {{column_name}} as source_medium,
        {%- elif column_name == 'first_user_campaign_name' -%}
        {{column_name}} as campaign_name,
        {%- elif column_name == 'first_user_campaign_id' -%}
        {{column_name}} as campaign_id,
        {%- elif column_name == 'first_user_manual_ad_content' -%}
        {{column_name}} as ad,
        {%- elif column_name == 'first_user_manual_term' -%}
        {{column_name}} as term,
        {%- else -%}
        {{column_name}}*/

        {%- endif -%}
    
    {%- endif -%}


{% endmacro -%}
