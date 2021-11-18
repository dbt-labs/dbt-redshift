{{
    config(
        materialized='view', backup=True
    )
}}

select 3
