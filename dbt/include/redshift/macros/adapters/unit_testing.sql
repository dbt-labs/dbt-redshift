{% macro redshift__validate_fixture_rows(rows, row_number) %}
  {%- if items is not none and items|length > 0 -%}
    {%- set some_value_is_none = false -%}
    {%- set row = items[0] -%}
    {%- for key, value in row.items() -%}
      {%- if value is none -%}
        {%- set some_value_is_none = true -%}
      {%- endif -%}
    {%- endfor -%}

    {%- if some_value_is_none -%}
      {%- set fixture_name = "expected output" if model.resource_type == 'unit_test' else ("'" ~ model.name ~ "'") -%}
      {{ exceptions.raise_compiler_error("Unit test fixture " ~ fixture_name ~ " in " ~ model.name ~ " does not have any row free of null values, which may cause type mismatch errors during unit test execution.") }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro -%}
