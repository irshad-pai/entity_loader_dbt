{{ config(materialized='table') }}

with source_data as (

    select 1 as id2,3 as id,4 as id3

)

select *
from source_data