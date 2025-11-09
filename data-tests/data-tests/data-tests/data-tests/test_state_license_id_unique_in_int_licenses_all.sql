-- data-tests/test_state_license_id_unique_in_int_licenses_all.sql
-- Fails if state_license_id is duplicated in the intermediate layer.

with dupes as (
    select
        state_license_id,
        count(*) as row_count
    from {{ ref('int_licenses_all') }}
    group by state_license_id
    having count(*) > 1
)

select * from dupes