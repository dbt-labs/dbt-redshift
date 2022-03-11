{{
    config(
        materialized='table', backup=False, dist='distkey'
    )
}}

select 1 as distkey