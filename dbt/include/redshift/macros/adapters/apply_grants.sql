{% macro get_users() %}
{% call statement('get_users_list', fetch_result=True) -%}
select
    distinct user_name
from svv_user_info
where
    user_name != current_user
    and superuser = false
  {% endcall %}

{{ return(load_result('get_users_list').table) }}
{% endmacro %}

{% macro redshift__get_show_grant_sql(relation) %}
{% set users_list = get_users() %}
{%- set users_list = users_list.columns[0].values() -%}
{%- set user_privilege_list = [] -%}
{% for username in users_list %}
    {{ user_privilege_list.append((username, 'select')) }}
    {{ user_privilege_list.append((username, 'insert')) }}
    {{ user_privilege_list.append((username, 'update')) }}
    {{ user_privilege_list.append((username, 'delete')) }}
    {{ user_privilege_list.append((username, 'references')) }}
{% endfor %}

{% for username, privilege in (user_privilege_list) %}
    select '{{ username }}' as grantee,
    '{{ privilege }}' as privilege_type
    where has_table_privilege('{{ username }}', '{{ relation }}', '{{ privilege }}')
    {% if not loop.last %}
    union all
    {% endif %}
{% endfor %}

{% endmacro %}
