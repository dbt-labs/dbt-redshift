{% macro redshift__no_svv_table_info_warning() %}

    {% set msg %}

    Warning: The database user "{{ target.user }}" has insufficient permissions to
    query the "svv_table_info" table. Please grant SELECT permissions on this table
    to the "{{ target.user }}" user to fetch extended table details from Redshift.

    {% endset %}

    {{ log(msg, info=True) }}

{% endmacro %}
