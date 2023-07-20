{% macro redshift__can_select_from(table_name) %}

  {%- call statement('has_table_privilege', fetch_result=True) -%}

    select has_table_privilege(current_user, '{{ table_name }}', 'SELECT') as can_select

  {%- endcall -%}

  {% set can_select = load_result('has_table_privilege').table[0]['can_select'] %}
  {{ return(can_select) }}

{% endmacro %}
