with dedup as (

    select
        *,
        row_number() over (
            partition by
                upper(trim(coalesce(state, ''))),
                coalesce(state_license_number, ''),
                coalesce(state_license_number_part_b, ''),
                upper(trim(coalesce(license_type_raw, '')))
            order by
                expiration_date_raw desc
        ) as rn
    from {{ ref('int_licenses_all') }}

),     -- start from unified licenses, one row per raw license record

final as (

    select
        -- Primary key for the table
        {{ dbt_utils.generate_surrogate_key([
            "upper(trim(coalesce(state, '')))",
            "coalesce(state_license_number, '')",
            "coalesce(state_license_number_part_b, '')",
            "upper(trim(coalesce(license_type_raw, '')))"
        ]) }}                                          as id,
        case
            when upper(state) in ('CA', 'CALIFORNIA', 'CALIFORNIA-RN') then 'CA'
            when upper(state) in ('MA', 'MASSACHUSETTS')               then 'MA'
            when upper(state) in ('WA', 'WASHINGTON')                  then 'WA'
            else state
        end as license_state,
        state_license_number                           as license_number,
        upper(trim(coalesce(license_type_raw, '')))    as license_type,

        case
            when upper(trim(license_status_raw)) in ('ACTIVE', 'A') then 'Active'
            when upper(trim(license_status_raw)) in ('INACTIVE', 'INACT', 'I') then 'Inactive'
            when upper(trim(license_status_raw)) in ('EXPIRED', 'E') then 'Expired'
            when upper(trim(license_status_raw)) in ('SUSPENDED', 'S') then 'Suspended'
            when license_status_raw is null or trim(license_status_raw) = '' then 'Unknown'
            else 'Unknown'
        end                                           as license_status,

        practitioner_first_name                       as first_name,
        practitioner_middle_name                      as middle_name,
        practitioner_last_name                        as last_name,

        -- keep raw date strings as issue/expiration dates (OK for this exercise)
        original_issue_date_raw                       as issue_date,
        expiration_date_raw                           as expiration_date,

        -- extra columns (not required by schema.yml, but useful for debugging)
        practitioner_full_name,
        state_source,
        state_license_number_part_b,
        address_city,
        address_state_raw                             as address_state,
        address_postal_code,
        birth_year,
        compact_status,
        active,
        state_license_id

    from dedup
    where rn = 1   -- one row per logical license

)

select * from final