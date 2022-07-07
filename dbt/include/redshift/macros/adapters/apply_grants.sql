{% macro redshift__get_show_grant_sql(relation) %}

with privileges as (

    -- valid options per https://docs.aws.amazon.com/redshift/latest/dg/r_HAS_TABLE_PRIVILEGE.html
    select 'select' as privilege
    union all
    select 'insert' as privilege
    union all
    select 'update' as privilege
    union all
    select 'delete' as privilege
    union all
    select 'references' as privilege

)

select
    u.usename as grantee,
    p.privilege
from pg_user u
cross join privileges p
where has_table_privilege(u.usename, '{{ relation }}', privilege)

{% endmacro %}
