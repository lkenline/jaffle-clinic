with base as (

    -- start from unified licenses table
    select *
    from {{ ref('int_licenses_all') }}

),

with_practitioner_id as (

    select
        *,
        -- NOTE: this expression must match the one used in int_practitioners_all
        {{ dbt_utils.generate_surrogate_key([
            "upper(trim(coalesce(practitioner_full_name, '')))",
            "cast(coalesce(birth_year, 0) as varchar)",
            "upper(trim(coalesce(state, '')))"
        ]) }} as practitioner_id
    from base

),

final as (

    select
        -- license-level primary key
        {{ dbt_utils.generate_surrogate_key([
            "upper(trim(coalesce(state, '')))",
            "coalesce(state_license_number, '')",
            "coalesce(state_license_number_part_b, '')",
            "upper(trim(coalesce(license_type_raw, '')))"
        ]) }} as id,

        practitioner_id,

        -- state + provenance
        state,
        state_source,

        -- identifiers
        state_license_number,
        state_license_number_part_b,

        -- normalized license type
        upper(trim(coalesce(license_type_raw, ''))) as license_type,

        -- normalized license status into canonical buckets
        case
            when upper(trim(license_status_raw)) in ('ACTIVE', 'A') then 'Active'
            when upper(trim(license_status_raw)) in ('INACTIVE', 'INACT', 'I') then 'Inactive'
            when upper(trim(license_status_raw)) in ('EXPIRED', 'E') then 'Expired'
            when upper(trim(license_status_raw)) in ('SUSPENDED', 'S') then 'Suspended'
            when license_status_raw is null or trim(license_status_raw) = '' then 'Unknown'
            else 'Unknown'
        end as license_status,

        -- keep raw dates (string) – we’ll parse flexibly in analysis queries
        original_issue_date_raw,
        expiration_date_raw,

        -- practitioner name fields (denormalized)
        practitioner_first_name,
        practitioner_middle_name,
        practitioner_last_name,
        practitioner_full_name,

        -- address
        address_city,
        address_state_raw as address_state,
        address_postal_code,

        -- misc useful fields
        birth_year,
        compact_status,
        active,
        state_license_id

    from with_practitioner_id

)

select * from final