{{
    config(
        materialized='table', backup=True, dist='distkey'
    )
}}

select 1 as distkey