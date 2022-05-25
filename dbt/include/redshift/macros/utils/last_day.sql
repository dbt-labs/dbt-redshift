{# redshift should use default instead of postgres #}
{% macro redshift__last_day(date, datepart) %}

    {{ return(dbt.default__last_day(date, datepart)) }}

{% endmacro %}
