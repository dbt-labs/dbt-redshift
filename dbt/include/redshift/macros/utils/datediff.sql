{#-- redshift should use default instead of postgres --#}
{% macro redshift__datediff(first_date, second_date, datepart) -%}
    {{ return(dbt.default__datediff(first_date, second_date, datepart)) }}
{%- endmacro %}
