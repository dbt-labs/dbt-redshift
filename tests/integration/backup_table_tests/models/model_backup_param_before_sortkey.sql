{{
    config(
        materialized='table', backup=False, sort='sortkey'
    )
}}

select 1 as sortkey