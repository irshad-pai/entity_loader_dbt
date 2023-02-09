{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-- Overriding this macro to support schema configuration in each model--#}
    {#-- This macro will use the schema provided in models by overriding schema mentioned in the profile.yml--#}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}