with source as (

    -- read from the seeded misc table
    select *
    from {{ ref('misc') }}

),

final as (

    select
        -- state metadata (varies per row here)
        license_state                    as state,
        'misc'                           as state_source,

        -- license identifiers
        cast(license as varchar)         as state_license_number,
        cast(null as varchar)            as state_license_number_part_b,

        -- license type & status
        "type"                           as license_type_raw,
        license_status                   as license_status_raw,

        -- dates (keep raw strings for now)
        license_original_issue_date      as original_issue_date_raw,
        license_expiration_date          as expiration_date_raw,

        -- practitioner names
        null                             as practitioner_first_name,
        null                             as practitioner_middle_name,
        null                             as practitioner_last_name,
        name_on_license                  as practitioner_full_name,

        -- address (not present in misc data)
        null                             as address_city,
        license_state                    as address_state_raw,
        null                             as address_postal_code,

        -- extra raw fields that only exist in misc
        compact_status,
        advanced_practice_license_recognition_information,
        active,

        -- per-state license id
        {{ dbt_utils.generate_surrogate_key([
            'license_state',
            'license',
            '"type"'
        ]) }} as state_license_id

    from source

)

select * from final