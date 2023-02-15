{{ config
    (
        materialized='view',
        file_format= "iceberg"
    )
}}
{% set skew_factor= var('skew_factor')  %}

WITH ei_date_filtered AS(
    SELECT
    computer_name,
    event_timestamp_epoch
    FROM ei_test.win_data
    WHERE ingested_timestamp >= {{ var('start_epoch') }} and ingested_timestamp < {{ var('end_epoch') }}
),
apply_filter_condition AS(
    SELECT
     computer_name,
     event_timestamp_epoch
    FROM ei_date_filtered
    WHERE computer_name IS NOT NULL
),
add_core_fields AS(
    SELECT
     computer_name,
     event_timestamp_epoch,
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
    SELECT
    computer_name,
    event_timestamp_epoch
      first_seen,
         last_seen,
         updated_at,
         analysis_period_start,
         analysis_period_end,
         computer_name,
         event_timestamp_epoch,
         primary_key,
         origin,
    SHA2(NULLIF(CONCAT_WS('||',IFNULL(NULLIF(UPPER(TRIM(CAST(primary_key AS STRING))), ''), '^^'),IFNULL(NULLIF(UPPER(TRIM(CAST(origin AS STRING))), ''), '^^')),'^^||^^'),256) as p_id
    FROM add_core_fields
),
apply_transformation AS (
    SELECT
      computer_name,
        event_timestamp_epoch
          first_seen,
             last_seen,
             updated_at,
             analysis_period_start,
             analysis_period_end,
             computer_name,
             event_timestamp_epoch,
             primary_key,
             origin,
             p_id,
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
)
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
