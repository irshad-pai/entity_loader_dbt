


{{ config(materialized='table') }}

with source_data as (

    {{ dbt_utils.union_relations(
    relations=[ref('my_first_dbt_model'),ref('my_second_dbt_model')]
) }}



)

select *
from source_data