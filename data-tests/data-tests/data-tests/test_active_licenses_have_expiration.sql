-- data-tests/test_active_licenses_have_expiration.sql
-- Fails if any Active license is missing an expiration_date.

select
    id,
    license_state,
    license_number,
    license_status,
    expiration_date
from {{ ref('licenses') }}
where
    license_status = 'Active'
    and (
        expiration_date is null
        or trim(expiration_date) = ''
    )