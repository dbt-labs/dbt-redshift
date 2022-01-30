{{
    config(
        materialized='table', backup=True, sort='distkey'
    )
}}

select 1 as sortkey