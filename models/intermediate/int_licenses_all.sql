with california as (

    select
        state,
        state_source,
        state_license_number,
        state_license_number_part_b,
        license_type_raw,
        license_status_raw,

        -- already raw strings
        original_issue_date_raw,
        expiration_date_raw,

        practitioner_first_name,
        practitioner_middle_name,
        practitioner_last_name,
        practitioner_full_name,

        address_city,
        address_state_raw,
        address_postal_code,

        -- only WA has birth_year; CA doesn't
        cast(null as integer)                as birth_year,

        -- only misc has compact_status / active; CA doesn't
        cast(null as varchar)                as compact_status,
        cast(null as varchar)                as active,

        state_license_id

    from {{ ref('stg_california') }}

),

massachusetts as (

    select
        state,
        state_source,
        state_license_number,
        state_license_number_part_b,
        license_type_raw,
        license_status_raw,

        original_issue_date_raw,
        expiration_date_raw,

        practitioner_first_name,
        practitioner_middle_name,
        practitioner_last_name,
        practitioner_full_name,

        address_city,
        address_state_raw,
        address_postal_code,

        cast(null as integer)                as birth_year,
        cast(null as varchar)                as compact_status,
        cast(null as varchar)                as active,

        state_license_id

    from {{ ref('stg_massachusetts') }}

),

washington as (

    select
        state,
        state_source,
        state_license_number,
        state_license_number_part_b,
        license_type_raw,
        license_status_raw,

        original_issue_date_raw,
        expiration_date_raw,

        practitioner_first_name,
        practitioner_middle_name,
        practitioner_last_name,
        practitioner_full_name,

        address_city,
        address_state_raw,
        address_postal_code,

        birth_year,
        cast(null as varchar)                as compact_status,
        cast(null as varchar)                as active,

        state_license_id

    from {{ ref('stg_washington') }}

),

misc as (

    select
        state,
        state_source,
        state_license_number,
        state_license_number_part_b,
        license_type_raw,
        license_status_raw,

        original_issue_date_raw,
        expiration_date_raw,

        practitioner_first_name,
        practitioner_middle_name,
        practitioner_last_name,
        practitioner_full_name,

        address_city,
        address_state_raw,
        address_postal_code,

        cast(null as integer)                as birth_year,
        compact_status,
        active,

        state_license_id

    from {{ ref('stg_misc') }}

),

all_licenses as (

    select * from california
    union all
    select * from massachusetts
    union all
    select * from washington
    union all
    select * from misc

)

select * from all_licenses