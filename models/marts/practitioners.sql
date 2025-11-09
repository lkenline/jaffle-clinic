with practitioners_base as (

    select *
    from {{ ref('int_practitioners_all') }}

),     -- one row per practitioner from the intermediate table

licenses_with_practitioner_id as (

    select
        {{ dbt_utils.generate_surrogate_key([
            "upper(trim(coalesce(practitioner_full_name, '')))",
            "cast(coalesce(birth_year, 0) as varchar)",
            "upper(trim(coalesce(state, '')))"
        ]) }}                                                    as practitioner_id,

        upper(trim(coalesce(license_type_raw, '')))              as license_type,
        state                                                    as license_state,

        case
            when upper(trim(license_status_raw)) in ('ACTIVE', 'A') then 'Active'
            when upper(trim(license_status_raw)) in ('INACTIVE', 'INACT', 'I') then 'Inactive'
            when upper(trim(license_status_raw)) in ('EXPIRED', 'E') then 'Expired'
            when upper(trim(license_status_raw)) in ('SUSPENDED', 'S') then 'Suspended'
            when license_status_raw is null or trim(license_status_raw) = '' then 'Unknown'
            else 'Unknown'
        end                                                      as license_status
    from {{ ref('int_licenses_all') }}

),     -- rebuild practitioner_id on the licenses side using the same logic

license_agg as (

    -- aggregate licenses per practitioner_id
    select
        practitioner_id,  -- aggregate licenses per practitioner_id
        string_agg(
            distinct license_type,
            ', '
        )                                          as license_types,         -- unique list of license types held by the provider
        string_agg(
            distinct license_state,
            ', '
        ) filter (where license_status = 'Active')
                                                   as authorized_to_practice_in         -- unique list of states where provider can currently practice (Active licenses)
    from licenses_with_practitioner_id
    group by practitioner_id

),

final as (

    select
        p.practitioner_id          as id,
        p.practitioner_first_name  as first_name,
        p.practitioner_middle_name as middle_name,
        p.practitioner_last_name   as last_name,
        coalesce(l.license_types, '')              as license_types,
        coalesce(l.authorized_to_practice_in, '')  as authorized_to_practice_in

    from practitioners_base p
    left join license_agg l
        on p.practitioner_id = l.practitioner_id

)

select * from final