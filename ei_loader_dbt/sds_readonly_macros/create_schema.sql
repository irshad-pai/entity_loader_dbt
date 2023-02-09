{% macro spark__create_schema(relation) -%}
  {#-- Overriding create schema sql query since Apache Spark adapter seems to be creating schema without the location with default macro--#}

  {%- call statement('create_schema') -%}
    create schema if not exists {{var("schema_name")}} LOCATION '{{var("schema_location")}}'
  {% endcall %}
{% endmacro %}