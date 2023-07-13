{% macro redshift__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {%- set predicates = [] -%}
    {% if incremental_predicates is not none %}
        {%- set incremental_predicates_list = [] + incremental_predicates -%}
        {%- for pred in incremental_predicates_list -%}
            {% if "DBT_INTERNAL_DEST." in pred %}
                {%- set pred =  pred | replace("DBT_INTERNAL_DEST.", target ~ "." ) -%}
            {% endif %}
            {% if "dbt_internal_dest." in pred %}
                {%- set pred =  pred | replace("dbt_internal_dest.", target ~ "." ) -%}
            {% endif %}
            {% do predicates.append(pred) %}
        {% endfor %}
    {% endif %}

    {%- set source_cols_csv = get_quoted_csv_w_alias(dest_columns | map(attribute="name"), alias="DBT_INTERNAL_SOURCE.") -%}

    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = {{ target }}.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = {{ target }}.{{ unique_key }}
            {% endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }}
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{"(" ~ predicates | join(") and (") ~ ")"}}

    {% if unique_key %}
    when matched then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {% endif %}
        {% endfor %}
    {% endif %}

    when not matched then insert values
        ({{ source_cols_csv }})

{% endmacro %}


{% macro get_quoted_csv_w_alias(column_names, alias="") %}

    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- do quoted.append(alias ~ adapter.quote(col)) -%}
    {%- endfor %}

    {%- set cols_csv = quoted | join(', ') -%}
    {{ return(cols_csv) }}

{% endmacro %}
