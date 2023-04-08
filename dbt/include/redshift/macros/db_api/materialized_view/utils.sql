{% macro redshift__db_api__utils__dist_clause(dist_style, dist_key) %}
    {% set dist_key = dist_key.strip().lower() %}

    {% if dist_style in ['all', 'even'] %}
        {% do return('diststyle ' ~ {{ dist_style }}) %}
    {% elif dist == 'auto' %}
        {% do return ('') %}
    {% else %}
        {% do return('diststyle key distkey (' ~ {{ dist_key }} ~ ')') %}
    {% endif %}
{% endmacro %}


{% macro redshift__db_api__utils__sort_clause(sort_type, sort_key) %}
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

    {% do return(sort_clause) %}
{% endmacro %}


{% macro redshift__db_api__utils__backup_clause(backup) %}
    {% if backup is sameas true %}
        {% do return('backup yes') %}
    {% elif backup is sameas false %}
        {% do return('backup no') %}
    {% else %}
        {% do return('') %}
    {% endif %}
{% endmacro %}


{% macro redshift__db_api__utils__auto_refresh_clause(auto_refresh) %}
    {% if auto_refresh is sameas true %}
        {% do return('auto refresh yes') %}
    {% elif auto_refresh is sameas false %}
        {% do return('auto refresh no') %}
    {% else %}
        {% do return('') %}
    {% endif %}
{% endmacro %}
