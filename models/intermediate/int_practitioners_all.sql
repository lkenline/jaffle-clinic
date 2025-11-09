with base as (

    -- each row here is a license from any state
    select *
    from {{ ref('int_licenses_all') }}

),

with_practitioner_id as (

    select
        *,
        -- generate a stable practitioner_id
        -- (same logic we'll reuse in the marts)
        {{ dbt_utils.generate_surrogate_key([
            "upper(trim(coalesce(practitioner_full_name, '')))",
            "cast(coalesce(birth_year, 0) as varchar)",
            "upper(trim(coalesce(state, '')))"
        ]) }} as practitioner_id
    from base

),

final as (

    -- one row per practitioner
    select
        practitioner_id,

        -- names: pick "max" as a deterministic representative
        max(practitioner_full_name)        as practitioner_full_name,
        max(practitioner_first_name)       as practitioner_first_name,
        max(practitioner_middle_name)      as practitioner_middle_name,
        max(practitioner_last_name)        as practitioner_last_name,

        -- demographics
        max(birth_year)                    as birth_year,

        -- location: pick a representative state & address
        max(state)                         as primary_state,
        max(address_city)                  as primary_city,
        max(address_state_raw)             as primary_address_state,
        max(address_postal_code)           as primary_postal_code,

        -- misc fields that might be useful
        max(compact_status)                as compact_status_any,
        max(active)                        as active_any,

        -- small summary metrics
        count(*)                           as license_count

    from with_practitioner_id
    group by practitioner_id

)

select * from final