with source as (

    -- read from the seeded massachusetts table
    select *
    from {{ ref('massachusetts') }}

),

final as (

    select
        -- state metadata
        'MA'              as state,
        'massachusetts'   as state_source,

        -- license identifiers
        cast(license_number as varchar)  as state_license_number,
        cast(null as varchar)            as state_license_number_part_b,

        -- license type & status
        license_type                     as license_type_raw,
        status                           as license_status_raw,

        -- keep dates as raw strings for now
        issue_date                       as original_issue_date_raw,
        expiration_date                  as expiration_date_raw,

        -- practitioner names
        -- we keep the full name string and leave first/middle/last null for now
        null                             as practitioner_first_name,
        null                             as practitioner_middle_name,
        null                             as practitioner_last_name,
        licensee_name                    as practitioner_full_name,

        -- address info (all in one field here)
        null                             as address_city,
        'MA'                             as address_state_raw,
        null                             as address_postal_code,
        address                          as address_full,

        -- extra useful raw fields
        drug_schedules,
        revoked_date,
        associated_records,
        void_date,
        certification,
        retirement_date,
        aprn_supervision,
        surrendered_date,
        compliance_actions,
        fines,

        -- per-state license id (surrogate key)
        {{ dbt_utils.generate_surrogate_key([
            "'MA'",
            'license_number',
            'license_type'
        ]) }} as state_license_id

    from source

)

select * from final