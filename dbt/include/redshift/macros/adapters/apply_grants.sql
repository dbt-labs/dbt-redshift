{# ------- DCL STATEMENT TEMPLATES ------- #}

{%- macro redshift__get_grant_sql(relation, privilege, grantee_dict) -%}
    {#-- generates a multiple-grantees grant privilege statement --#}
    grant {{privilege}} on {{relation}} to
    {%- for grantee_type, grantees in grantee_dict.items() -%}
    {%- if grantee_type=='user' and grantees -%}
        {{ " " + (grantees | join(', ')) }}
    {%- elif grantee_type=='group' and grantees -%}
        {{ " " +("group " + grantees | join(', group ')) }}
    {%- elif grantee_type=='role' and grantees -%}
        {{ " " + ("role " + grantees | join(', role ')) }}
    {%- endif -%}
    {%- if not loop.last -%}
        ,
    {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}

{%- macro redshift__get_revoke_sql(relation, privilege, revokee_dict) -%}
    {#-- generates a multiple-grantees revoke privilege statement --#}
    revoke {{privilege}} on {{relation}} from
    {%- for revokee_type, revokees in revokee_dict.items() -%}
    {%- if revokee_type=='user' and revokees -%}
        {{ " " + (revokees | join(', ')) }}
    {%- elif revokee_type=='group' and revokees -%}
        {{ " " +("group " + revokees | join(', group ')) }}
    {%- elif revokee_type=='role' and revokees -%}
        {{ " " + ("role " + revokees | join(', role ')) }}
    {%- endif -%}
    {%- if not loop.last -%}
        ,
    {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}


{% macro redshift__get_show_grant_sql(relation) %}
{#-- shows the privilege grants on a table for users, groups, and roles --#}
with privileges as (

    -- valid options per https://docs.aws.amazon.com/redshift/latest/dg/r_HAS_TABLE_PRIVILEGE.html
    select 'select' as privilege_type
    union all
    select 'insert' as privilege_type
    union all
    select 'update' as privilege_type
    union all
    select 'delete' as privilege_type
    union all
    select 'references' as privilege_type

)

select
    'user' as grantee_type,
    u.usename as grantee,
    p.privilege_type
from pg_user u
cross join privileges p
where has_table_privilege(u.usename, '{{ relation }}', privilege_type)
    and u.usename != current_user
    and not u.usesuper

union all
-- check that group has table privilege
select
    'group' as grantee_type,
    g.groname as grantee,
    p.privilege_type
from pg_group g
cross join privileges p
where exists(
    select *
    from information_schema.table_privileges tp
    where tp.grantee=g.groname
    and tp.table_schema=replace(split_part('{{ relation }}', '.', 2), '"', '')
    and tp.table_name=replace(split_part('{{ relation }}', '.', 3), '"', '')
    and LOWER(tp.privilege_type)=p.privilege_type
)

union all
-- check that role has table privilege
select
    'role' as grantee_type,
    r.role_name as grantee,
    p.privilege_type
from svv_roles r
cross join privileges p
where exists(
    select *
    from svv_relation_privileges rp
    where rp.identity_name=r.role_name
    and rp.namespace_name=replace(split_part('{{ relation }}', '.', 2), '"', '')
    and rp.relation_name=replace(split_part('{{ relation }}', '.', 3), '"', '')
    and LOWER(rp.privilege_type)=p.privilege_type
)

{% endmacro %}

{% macro redshift__apply_grants(relation, grant_config, should_revoke=True) %}
    {#-- Override for apply grants --#}
    {#-- If grant_config is {} or None, this is a no-op --#}
    {% if grant_config %}
        {#-- change grant_config to Dict[str, Dict[str, List[str]] format --#}
        {% set grant_config = adapter.process_grant_dicts(grant_config) %}

        {% if should_revoke %}
            {#-- We think that there is a chance that grants are carried over. --#}
            {#-- Show the current grants for users, roles, and groups and calculate the diffs. --#}
            {% set current_grants_table = run_query(get_show_grant_sql(relation)) %}
            {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
            {% set needs_granting = adapter.diff_of_two_nested_dicts(grant_config, current_grants_dict) %}
            {% set needs_revoking = adapter.diff_of_two_nested_dicts(current_grants_dict, grant_config) %}
            {% if not (needs_granting or needs_revoking) %}
                {{ log('On ' ~ relation ~': All grants are in place, no revocation or granting needed.')}}
            {% endif %}
        {% else %}
            {#-- We don't think there's any chance of previous grants having carried over. --#}
            {#-- Jump straight to granting what the user has configured. --#}
            {% set needs_revoking = {} %}
            {% set needs_granting = grant_config %}
        {% endif %}
        {% if needs_granting or needs_revoking %}
            {% set revoke_statement_list = get_dcl_statement_list(relation, needs_revoking, get_revoke_sql) %}
            {% set grant_statement_list = get_dcl_statement_list(relation, needs_granting, get_grant_sql) %}
            {% set dcl_statement_list = revoke_statement_list + grant_statement_list %}
            {% if dcl_statement_list %}
                {{ call_dcl_statements(dcl_statement_list) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
