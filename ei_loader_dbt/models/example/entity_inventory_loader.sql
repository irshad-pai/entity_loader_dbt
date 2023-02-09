{{ config
    (
        materialized='incremental',
        file_format= "iceberg",
        post_hook = "ALTER TABLE ei_test.entity_inventory_loader SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')"
    )
}}
{% set skew_factor= var('skew_factor')  %}
{% if is_incremental() %}
    {% set relations_input= [ref('entity_inventory_loader_view'), this] %}
{% else %}
    {% set relations_input= [ref('entity_inventory_loader_view')] %}
{% endif %}
WITH load_inventory AS(
    {{ dbt_utils.union_relations(relations=relations_input) }}
),
{% if  skew_factor > 0   %}
generate_salt_id as (
    select
    *,
    cast(( {{ var('skew_factor') }}* rand()) as int) as salt_id
    FROM load_inventory
),
generate_salted_primary_key as (
    select
    *,
    concat('_',primary_key,salt_id)  as  salted_primary_key
    FROM generate_salt_id
),
build_latest_inevntory_with_salting AS(
    SELECT
    origin,
    class,
    event_code,
    status,
    LAST_VALUE(first_seen) over(partition by salted_primary_key order by last_seen DESC) as first_seen,
    FIRST_VALUE(last_seen) over(partition by salted_primary_key order by last_seen DESC) as last_seen,
    FIRST_VALUE(updated_at) over(partition by salted_primary_key order by last_seen DESC) as updated_at,
    FIRST_VALUE(analysis_period_start) over(partition by salted_primary_key order by last_seen DESC) as analysis_period_start,
    FIRST_VALUE(analysis_period_end) over(partition by salted_primary_key order by last_seen DESC) as analysis_period_end,
    FIRST_VALUE(computer_name) over(partition by salted_primary_key order by last_seen DESC) as computer_name,
    FIRST_VALUE(event_timestamp_epoch) over(partition by salted_primary_key order by last_seen DESC) as event_timestamp_epoch,
    FIRST_VALUE(salted_primary_key) over(partition by salted_primary_key order by last_seen DESC) as salted_primary_key,
    FIRST_VALUE(primary_key) over(partition by primary_key order by last_seen DESC) as primary_key,
    FIRST_VALUE(p_id) over(partition by salted_primary_key order by last_seen DESC) as p_id,
    FIRST_VALUE(display_label) over(partition by salted_primary_key order by last_seen DESC) as display_label,
    FIRST_VALUE(host_name) over(partition by salted_primary_key order by last_seen DESC) as host_name,
    FIRST_VALUE(fqdn) over(partition by salted_primary_key order by last_seen DESC) as fqdn,
    FIRST_VALUE(host_type) over(partition by salted_primary_key order by last_seen DESC) as host_type,
    FIRST_VALUE(last_login) over(partition by salted_primary_key order by last_seen DESC) as last_login,
    FIRST_VALUE(activity_status) over(partition by salted_primary_key order by last_seen DESC) as activity_status,
    FIRST_VALUE(last_active_date) over(partition by salted_primary_key order by last_seen DESC) as last_active_date,
    ROW_NUMBER() over(partition by salted_primary_key order by last_seen DESC) as row_number
    FROM generate_salted_primary_key
),
filter_row_number_salted as(
    select
      origin,
         class,
         event_code,
         status,
         first_seen,
         last_seen,
         updated_at,
         analysis_period_start,
         analysis_period_end,
         computer_name,
         event_timestamp_epoch,
         primary_key,
         p_id,
         display_label,
         host_name,
         fqdn,
         host_type,
         last_login,
         activity_status,
         last_active_date
    from build_latest_inevntory_with_salting
    where row_number=1
),
{% endif %}
build_latest_inevntory_without_salting AS(
    SELECT
    origin,
    class,
    event_code,
    status,
    LAST_VALUE(first_seen) over(partition by primary_key order by last_seen DESC) as first_seen,
    FIRST_VALUE(last_seen) over(partition by primary_key order by last_seen DESC) as last_seen,
    FIRST_VALUE(updated_at) over(partition by primary_key order by last_seen DESC) as updated_at,
    FIRST_VALUE(analysis_period_start) over(partition by primary_key order by last_seen DESC) as analysis_period_start,
    FIRST_VALUE(analysis_period_end) over(partition by primary_key order by last_seen DESC) as analysis_period_end,
    FIRST_VALUE(computer_name) over(partition by primary_key order by last_seen DESC) as computer_name,
    FIRST_VALUE(event_timestamp_epoch) over(partition by primary_key order by last_seen DESC) as event_timestamp_epoch,
    FIRST_VALUE(primary_key) over(partition by primary_key order by last_seen DESC) as primary_key,
    FIRST_VALUE(p_id) over(partition by primary_key order by last_seen DESC) as p_id,
    FIRST_VALUE(display_label) over(partition by primary_key order by last_seen DESC) as display_label,
    FIRST_VALUE(host_name) over(partition by primary_key order by last_seen DESC) as host_name,
    FIRST_VALUE(fqdn) over(partition by primary_key order by last_seen DESC) as fqdn,
    FIRST_VALUE(host_type) over(partition by primary_key order by last_seen DESC) as host_type,
    FIRST_VALUE(last_login) over(partition by primary_key order by last_seen DESC) as last_login,
    FIRST_VALUE(activity_status) over(partition by primary_key order by last_seen DESC) as activity_status,
    FIRST_VALUE(last_active_date) over(partition by primary_key order by last_seen DESC) as last_active_date,
    ROW_NUMBER() over(partition by primary_key order by last_seen DESC) as row_number
    {% if  skew_factor > 0 %}
    FROM filter_row_number_salted
    {% else %}
    FROM load_inventory
    {% endif %}
),
filter_row_number as(
    select
      origin,
         class,
         event_code,
         status,
         first_seen,
         last_seen,
         updated_at,
         analysis_period_start,
         analysis_period_end,
         computer_name,
         event_timestamp_epoch,
         primary_key,
         p_id,
         display_label,
         host_name,
         fqdn,
         host_type,
         last_login,
         activity_status,
         last_active_date
    from build_latest_inevntory_without_salting
    where row_number=1
)
select * from filter_row_number