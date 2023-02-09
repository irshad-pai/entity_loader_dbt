{% macro get_insert_overwrite_sql_obsolete(source_relation, target_relation) %}
    {#-- Overriding insert overrite sql query since Apache Spark adapter seems to be producing error with default macro while using Iceberg file format--#}
    {#-- So dest_columns taking columns from source_relation instead of target_relation since we found out that target_relation is not getting updated 
    even after the table is altered with sql query. The source_relation will contanis the columns from the model we created.--#}

    {%- set dest_columns = adapter.get_columns_in_relation(source_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}


{% macro get_insert_into_sql(source_relation, target_relation) %}
    {#-- Overriding insert sql query since Apache Spark adapter seems to be producing error with default macro while using Iceberg file format--#}
    {#-- So dest_columns taking columns from source_relation instead of target_relation since we found out that target_relation is not getting updated 
    even after the table is altered with sql query. The source_relation will contanis the columns from the model we created.--#}

    {%- set dest_columns = adapter.get_columns_in_relation(source_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}
