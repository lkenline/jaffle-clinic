with source as (

    -- read from the seeded washington table
    select *
    from {{ ref('washington') }}

),

final as (

    select
        -- state metadata
        'WA'            as state,
        'washington'    as state_source,

        -- license identifiers
        cast(CredentialNumber as varchar)  as state_license_number,
        cast(null as varchar)              as state_license_number_part_b,

        -- license type & status
        CredentialType                     as license_type_raw,
        Status                             as license_status_raw,

        -- dates (keep as text for now; can parse later)
        cast(FirstIssueDate as varchar)    as original_issue_date_raw,
        cast(ExpirationDate as varchar)    as expiration_date_raw,

        -- practitioner names
        FirstName                          as practitioner_first_name,
        MiddleName                         as practitioner_middle_name,
        LastName                           as practitioner_last_name,
        trim(
            concat_ws(
                ' ',
                FirstName,
                nullif(MiddleName, ''),
                LastName
            )
        )                                  as practitioner_full_name,

        -- demographics
        BirthYear                          as birth_year,     -- 👈 important alias

        -- address (not present here)
        null                               as address_city,
        'WA'                               as address_state_raw,
        null                               as address_postal_code,

        -- extra raw fields
        LastIssueDate,
        ActionTaken,
        CEDueDate,

        -- per-state license id
        {{ dbt_utils.generate_surrogate_key([
            "'WA'",
            'CredentialNumber',
            'CredentialType'
        ]) }} as state_license_id

    from source

)

select * from final