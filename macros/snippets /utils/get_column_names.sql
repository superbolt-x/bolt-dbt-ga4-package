{% macro get_column_names(model_name, regex='+*') %}
    {%- set columns = adapter.get_columns_in_relation(ref(model_name)) -%}
    {%- set column_names = [] -%}
    {%- set re = modules.re %}
    
    {%- for column in columns %}
          {%- if not re.match(regex, column.name) %}
            {%- set _ = column_names.append(column.name) %}
        {%- endif %}
    {%- endfor %}
    
    {{ column_names}}
{% endmacro %}
