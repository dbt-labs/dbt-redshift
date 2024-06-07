{#- /*
While Redshift supports any_value, Postgres does not support any_value.
This mimics the previous behavior before decoupling dbt-redshift from dbt-postgres.
*/ -#}

{% macro redshift__any_value(expression) -%}

    min({{ expression }})

{%- endmacro %}
