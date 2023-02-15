{{ config
    (
        materialized='incremental',
        file_format= "iceberg",
        post_hook = "ALTER TABLE ei_test.entity_inventory_loader SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')"
    )
}}
{% set skew_factor= var('skew_factor')  %}

WITH ei_date_filtered AS(
    SELECT *
    FROM ei_test.win_data_250
    WHERE ingested_timestamp >= {{ var('start_epoch') }} and ingested_timestamp < {{ var('end_epoch') }}
),
apply_filter_condition AS(
    SELECT *
    FROM ei_date_filtered
    WHERE computer_name IS NOT NULL
),
add_core_fields AS(
    SELECT *,
    {{ var('end_epoch') }} as updated_at,
     {{ var('start_epoch') }} as analysis_period_start,
     {{ var('end_epoch') }} as analysis_period_end,
    'WinEvents' as origin,
    computer_name as primary_key,
    event_timestamp_epoch as first_seen,
    event_timestamp_epoch as last_seen
    FROM apply_filter_condition
),
generate_pid AS(
    SELECT *,
    SHA2(NULLIF(CONCAT_WS('||',IFNULL(NULLIF(UPPER(TRIM(CAST(primary_key AS STRING))), ''), '^^'),IFNULL(NULLIF(UPPER(TRIM(CAST(origin AS STRING))), ''), '^^')),'^^||^^'),256) as p_id
    FROM add_core_fields
),
apply_transformation AS (
    SELECT *,
    'Host' as class,
     COALESCE((CASE WHEN TRIM(LOWER(computer_name)) LIKE '%.com' OR  TRIM(LOWER(computer_name)) LIKE '%.net' OR TRIM(LOWER(computer_name)) LIKE '%.ads' OR TRIM(LOWER(computer_name)) LIKE '%.uk' OR TRIM(LOWER(computer_name)) LIKE '%.org' OR TRIM(LOWER(computer_name)) LIKE '%.edu' OR TRIM(LOWER(computer_name)) LIKE '%.gov' OR TRIM(LOWER(computer_name)) LIKE '%.info' OR TRIM(LOWER(computer_name)) LIKE '%.in' OR TRIM(LOWER(computer_name)) LIKE '%.azure' THEN LOWER(computer_name) ELSE NULL END),UPPER(REGEXP_EXTRACT(computer_name, '^([^.]+)')),computer_name) as display_label,
     UPPER(REGEXP_EXTRACT(computer_name, '^([^.]+)')) as host_name,
     CASE WHEN TRIM(LOWER(computer_name)) LIKE '%.com' OR  TRIM(LOWER(computer_name)) LIKE '%.net' OR TRIM(LOWER(computer_name)) LIKE '%.ads' OR TRIM(LOWER(computer_name)) LIKE '%.uk' OR TRIM(LOWER(computer_name)) LIKE '%.org' OR TRIM(LOWER(computer_name)) LIKE '%.edu' OR TRIM(LOWER(computer_name)) LIKE '%.gov' OR TRIM(LOWER(computer_name)) LIKE '%.info' OR TRIM(LOWER(computer_name)) LIKE '%.in' OR TRIM(LOWER(computer_name)) LIKE '%.azure' THEN LOWER(computer_name) ELSE NULL END as fqdn,
     CASE WHEN computer_name LIKE '%DC-%' OR computer_name LIKE '%-DC-%' OR computer_name LIKE '%-DC%' THEN 'Server' END as host_type,
     '4624' as event_code,
     event_timestamp_epoch as last_login,
     CASE WHEN DATEDIFF(FROM_UNIXTIME(updated_at/1000),FROM_UNIXTIME(last_seen/1000)) > 180 THEN 'InActive' ELSE 'Active' END as activity_status,
     'Active' as status,
     last_seen as last_active_date
     FROM generate_pid
),
select_field as (
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
     FROM apply_transformation
),
load_inventory AS(
    select *
    FROM select_field
    {% if is_incremental() %}
    UNION ALL
    select *
    FROM {{ this }}
    {% endif %}
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