{% macro redshift__get_relation_last_modified(information_schema, relations) -%}

    {%- call statement('last_modified', fetch_result=True) -%}
        select table_schema as schema,
               table_name as identifier,
               null as last_modified,  -- svv_tables does not have this, research where to get this information
               {{ current_timestamp() }} as snapshotted_at
        from svv_tables
        where (
            {%- for relation in relations -%}
                (
                    upper(table_schema) = upper('{{ relation.schema }}')
                and upper(table_name) = upper('{{ relation.identifier }}')
                )
                {%- if not loop.last %} or {% endif -%}
            {%- endfor -%}
        )
    {%- endcall -%}

    {{ return(load_result('last_modified')) }}

{% endmacro %}
