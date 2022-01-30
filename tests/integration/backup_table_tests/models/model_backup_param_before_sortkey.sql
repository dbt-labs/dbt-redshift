{{
    config(
        materialized='table', backup=True, sort='sortkey'
    )
}}

select 1 as sortkey