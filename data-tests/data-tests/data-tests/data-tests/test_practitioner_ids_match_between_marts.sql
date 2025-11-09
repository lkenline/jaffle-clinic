-- data-tests/test_practitioner_ids_match_between_marts.sql
-- Fails if practitioner IDs implied by licenses and practitioners don't line up.

with licenses_with_practitioner_id as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "upper(trim(coalesce(practitioner_full_name, '')))",
            "cast(coalesce(birth_year, 0) as varchar)",
            "upper(trim(coalesce(license_state, '')))"
        ]) }} as practitioner_id
    from {{ ref('licenses') }}
),

from_licenses as (
    select distinct practitioner_id
    from licenses_with_practitioner_id
),

from_practitioners as (
    select id as practitioner_id
    from {{ ref('practitioners') }}
),

only_in_licenses as (
    select
        l.practitioner_id,
        'in_licenses_not_practitioners' as reason
    from from_licenses l
    left join from_practitioners p using (practitioner_id)
    where p.practitioner_id is null
),

only_in_practitioners as (
    select
        p.practitioner_id,
        'in_practitioners_not_licenses' as reason
    from from_practitioners p
    left join from_licenses l using (practitioner_id)
    where l.practitioner_id is null
),

mismatches as (
    select * from only_in_licenses
    union all
    select * from only_in_practitioners
)

select * from mismatches