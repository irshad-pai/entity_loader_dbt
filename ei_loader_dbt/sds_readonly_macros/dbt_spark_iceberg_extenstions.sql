{% macro dbt_spark_validate_get_file_format(raw_file_format) %}
  {#-- Validate the file format #}
  {#-- Overriding this macro to add Iceberg support for Apache Spark adapter--#}
  
  {% set accepted_formats = ['text', 'csv', 'json', 'jdbc', 'parquet', 'orc', 'hive', 'delta', 'libsvm', 'hudi','iceberg'] %}

  {% set invalid_file_format_msg -%}
    Invalid file format provided: {{ raw_file_format }}
    Expected one of: {{ accepted_formats | join(', ') }}
  {%- endset %}

  {% if raw_file_format not in accepted_formats %}
    {% do exceptions.raise_compiler_error(invalid_file_format_msg) %}
  {% endif %}

  {% do return(raw_file_format) %}
{% endmacro %}

{% macro get_insert_overwrite_sql(source_relation, target_relation) %}

    {%- set pai_open_table_format = env_var('PAI_OPEN_TABLE_FORMAT', 'iceberg') -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {% if pai_open_table_format == 'iceberg'  %}
      {# removed table from statement for iceberg #}
      insert overwrite {{ target_relation }}
      {# removed partition_cols for iceberg as well #}
    {% else %}
      insert overwrite table {{ target_relation }}
      {{ partition_cols(label="partition") }}
    {% endif %}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}