{% macro sort(sort_type, sort) %}
  {%- if sort is not none %}
      {{ sort_type | default('compound', boolean=true) }} sortkey(
      {%- if sort is string -%}
        {%- set sort = [sort] -%}
      {%- endif -%}
      {%- for item in sort -%}
        {{ item }}
        {%- if not loop.last -%},{%- endif -%}
      {%- endfor -%}
      )
  {%- endif %}
{%- endmacro -%}
