with source as (

    select *
    from {{ source('licenses', 'california') }}

),     -- raw table defined in models/staging/source.yml

filtered as (

    select *
    from source
    where indiv_or_org = 'I'

),     -- keep only individual practitioners (drop organization licenses)

final as (

    select
        -- basic state metadata
        'CA'           as state,
        'california'   as state_source,

        -- license identifiers
        cast(license_number as varchar)            as state_license_number,
        cast(license_number_part_b as varchar)     as state_license_number_part_b,

        -- raw license type & status from the feed
        coalesce(license_type, license_type_name)  as license_type_raw,
        license_status                             as license_status_raw,

        -- dates in MM-DD-YYYY format
        to_date(original_issue_date, 'MM-DD-YYYY') as original_issue_date,
        to_date(expiration_date,     'MM-DD-YYYY') as expiration_date,

        -- practitioner names
        first_name                                 as practitioner_first_name,
        middle_name                                as practitioner_middle_name,
        org_or_last_name                           as practitioner_last_name,
        trim(
            concat_ws(
                ' ',
                first_name,
                nullif(middle_name, ''),
                org_or_last_name
            )
        )                                          as practitioner_full_name,

        -- location info
        city                                       as address_city,
        state                                      as address_state,
        cast(coalesce(zip_code, zip) as varchar)   as address_postal_code,

        -- extra useful raw fields (optional but nice to keep)
        suffix,
        degree,
        school,
        year_graduated,
        county,
        license_type_code,
        speciality_code,
        agency_name,
        country,
        status_effective_date,
        address_line_1,
        address_line_2,
        address_line1      as address_line1_raw,
        address_line2      as address_line2_raw,
        relationship_name,
        relationship_name_part_b,
        original_isuue_date as original_issue_date_raw,

        -- per-state license id (used later as a building block for the final license_id)
        {{ dbt_utils.generate_surrogate_key([
            "'CA'",
            'license_number',
            'coalesce(license_type, license_type_name)'
        ]) }}                                    as state_license_id

    from filtered

)

select * from final