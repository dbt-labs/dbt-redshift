{% macro redshift__db_api__utils__dist_clause(dist_style, dist_key) %}
    {% if dist_style == 'auto' %}
        {% do return ('') %}
    {% elif dist_style in ['all', 'even'] %}
        {% do return('diststyle ' ~ {{ dist_style }}) %}
    {% elif dist_style == 'key' %}
        {% do return('diststyle key distkey (' ~ {{ dist_key.strip().lower() }} ~ ')') %}
    {% else %}
        {% do return ('') %}
    {% endif %}
{% endmacro %}


{% macro redshift__db_api__utils__sort_clause(sort_type, sort_key) %}

    {% if sort_key %}
        {% if sort_key is string %}
            {% set sort_key = [sort_key] %}
        {% endif %}

        {% set %}
            sort_clause = {{ sort_type | default('compound', boolean=true) }} sortkey(
                {% for field in sort_key %}
                    {{- field -}}{% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        {% endset %}
    {% else %}
        {% set sort_clause = '' %}
    {% endif %}

    {% do return(sort_clause) %}
{% endmacro %}
