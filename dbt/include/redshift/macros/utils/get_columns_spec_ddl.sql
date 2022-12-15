{% macro redshift__get_columns_spec_ddl() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
  {%- if config.get('constraints_enabled', False) %}
    {%- set user_provided_columns = model['columns'] -%}
    {%- set primary_keys = [] -%}
    {%- set ddl_lines = [] -%}

    {%- for i in user_provided_columns %}
      {%- set col = user_provided_columns[i] -%}
      {%- set constraints = col['constraints'] -%}
      {%- set ns = namespace(not_null_line = '') -%}

      {%- for constraint in constraints %}
        {%- if constraint == 'primary key' -%}
          {%- do primary_keys.append(col['name']) -%}
        {%- elif constraint == 'not null' %}
          {%- set ns.not_null_line = " not null" -%}
        {%- endif -%}
      {%- endfor -%}

      {%- set not_null_line = " not null" if not_null_col else "" -%}

      {%- set check = col['check'] -%}
      {%- if check -%}
        {{ exceptions.warn("We noticed you have `check` in your configs, these are NOT compatible with Redshift and will be ignored. See column `" ~ col['name'] ~ "`") }}
      {%- endif -%}

      {%- set col_line = col['name'] ~ " " ~ col['data_type'] ~ ns.not_null_line -%}
      {%- do ddl_lines.append(col_line) -%}
    {%- endfor %}

    {%- if primary_keys -%}
      {%- set primary_key_line = "primary key(" ~ primary_keys | join(", ") ~")" -%}
      {%- do ddl_lines.append(primary_key_line) -%}
    {%- endif -%}

    (
      {%- for line in ddl_lines %}
        {{ line }} {{ "," if not loop.last }}
      {%- endfor %}
    )

  {%- endif %}
{% endmacro %}
