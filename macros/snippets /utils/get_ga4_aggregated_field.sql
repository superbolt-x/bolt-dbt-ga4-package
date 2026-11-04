{%- macro get_ga4_aggregated_field(field, fields) -%}

{#- bounce_rate and average_session_duration are per-session averages: summing them over the rows of a week,
    month or campaign gives meaningless numbers (e.g. a 23.19 "bounce rate"). Weight them by sessions instead,
    which gives exactly bounced_sessions / sessions and session_duration / sessions. Every other field is summed. -#}

    {%- if field in ['bounce_rate', 'average_session_duration'] and 'sessions' in fields -%}
        COALESCE(SUM("{{ field }}" * "sessions")::float / NULLIF(SUM("sessions"),0),0) as "{{ field }}"
    {%- else -%}
        COALESCE(SUM("{{ field }}"),0) as "{{ field }}"
    {%- endif -%}

{% endmacro -%}
