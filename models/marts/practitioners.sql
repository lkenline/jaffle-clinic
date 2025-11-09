with practitioners_base as (

    -- one row per practitioner_id from the intermediate model
    select *
    from {{ ref('int_practitioners_all') }}

),

licenses_with_practitioner_id as (

    -- derive practitioner_id in the same way, from licenses side
    select
        {{ dbt_utils.generate_surrogate_key([
            "upper(trim(coalesce(practitioner_full_name, '')))",
            "cast(coalesce(birth_year, 0) as varchar)",
            "upper(trim(coalesce(state, '')))"
        ]) }} as practitioner_id,
        upper(trim(coalesce(license_type_raw, ''))) as license_type_upper
    from {{ ref('int_licenses_all') }}

),

license_agg as (

    select
        practitioner_id,
        count(*) as total_license_count,
        -- very simple heuristic: license_type containing 'REGISTERED NURSE'
        count(*) filter (where license_type_upper like '%REGISTERED NURSE%') as rn_license_count
    from licenses_with_practitioner_id
    group by practitioner_id

),

final as (

    select
        p.practitioner_id,

        -- names
        p.practitioner_full_name,
        p.practitioner_first_name,
        p.practitioner_middle_name,
        p.practitioner_last_name,

        -- demographics / primary location
        p.birth_year,
        p.primary_state,
        p.primary_city,
        p.primary_address_state,
        p.primary_postal_code,

        -- flags / summaries
        p.compact_status_any,
        p.active_any,
        coalesce(l.total_license_count, 0) as total_license_count,
        coalesce(l.rn_license_count, 0)    as rn_license_count,
        case
            when coalesce(l.rn_license_count, 0) > 0 then true
            else false
        end as has_registered_nurse_license

    from practitioners_base p
    left join license_agg l
        on p.practitioner_id = l.practitioner_id

)

select * from final