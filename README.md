## Overview

This project was fun. I did run into some issues getting my local environment set up at first (I should have taken the advice to use DuckDB right away), which cost me some time. Once I got past that, I tried to keep the project and documentation clear so it’s easy to follow.

## Approach

I modeled the project in four layers. The seed layer was already provided; I added staging, intermediate, and mart layers on top of it.

1. **Seeds (raw)**  
   - Loaded the four CSVs into DuckDB via dbt seeds, under the `main_raw` schema.

2. **Staging models (`models/staging`)**  
   At this layer, I try to keep transformations minimal and reversible: the goal is to clean and standardize, not to change the grain or business meaning yet.

   The four staging models are:

   - `stg_california`
   - `stg_massachusetts`
   - `stg_washington`
   - `stg_misc`

   Key things I did here:

   - **Filter to individual practitioners where needed**  
     The California feed includes both individual practitioners and organizations (e.g. facilities, groups) in the same table. Since the downstream marts (`practitioners` and `licenses`) are defined at the individual provider level, I explicitly filter `stg_california` to `indiv_or_org = 'I'` to drop organization-level licenses. This avoids polluting the practitioner mart with non-person entities and keeps the grain consistent across states.

   - **Standardize common fields across states**  
     Each state calls things slightly differently (e.g. `license_type_name` vs `license_type`, different status codes, different name fields). In staging I:
     - Aligned column names into a common set (e.g. `license_type_raw`, `license_status_raw`, `practitioner_first_name`, `practitioner_last_name`).
     - Added a normalized `state` field so downstream models don’t have to infer state from table names.
     - Preserved useful raw fields (like original text dates and free-text address fields) for later debugging.

   - **Normalize key attributes but keep them “raw-ish”**  
     I did light normalization that makes modeling easier without overfitting to one state’s rules:
     - Pulled out practitioner name components into consistent columns.
     - Captured the raw license type and status in `license_type_raw` and `license_status_raw`, deferring full normalization (e.g. mapping to `Active/Inactive/Expired`) to the mart layer.
     - Left dates as strings at this stage and only parse/interpret them in later layers where cross-state logic is clearer.

   Overall, the staging layer gives me a set of state-specific views that all “feel” the same structurally, while still being close enough to the source to debug issues or adjust assumptions later.

3. **Intermediate models (`models/intermediate`)**  

   - `int_licenses_all`: union of all state-level staging models into one long licenses table.  
     - Generated a per-state surrogate key `state_license_id` from state + license number + license type using `dbt_utils.generate_surrogate_key`. This gives me a stable identifier for a license as it appears in a specific state feed and makes it easier to reason about duplicates.
   - `int_practitioners_all`: deduped practitioners across licenses.  
     - Generated a `practitioner_id` from full name + birth year + state (approximate identity resolution).  
     - Aggregated per-practitioner metrics (e.g. total license count).

4. **Marts (`models/marts`)**  
   - `licenses`: one row per logical license.  
     - Deduplicated records where `(state, license_number, license_type)` are the same using a `row_number()` window and keeping `rn = 1`.  
     - Normalized `license_state` to 2-letter state codes (`CA`, `MA`, `WA`) even when the source provided values like `CALIFORNIA-RN` or `WASHINGTON`.  
     - Exposes the columns required by the exercise:  
       `id, license_state, license_number, license_type, license_status, first_name, middle_name, last_name, issue_date, expiration_date`.
   - `practitioners`: one row per provider.  
     - Exposes `id, first_name, middle_name, last_name, license_types, authorized_to_practice_in`.  
     - `license_types` is a distinct, comma-separated list of license types across all states.  
     - `authorized_to_practice_in` is a distinct list of state codes where the provider currently has at least one **Active** license.

## Assumptions

- A “Registered Nurse” is any license where the normalized `license_type` contains the string `REGISTERED NURSE` (case-insensitive).
- A “logical license” is identified by `(state, license_number, license_type)`. If multiple rows share that combination, they are treated as duplicates and collapsed in the `licenses` mart.
- Practitioners are approximated by full name + birth year + state rather than a true global identifier, which is sufficient for this exercise.
- Dates appear in multiple formats by state, so I keep the raw strings in the mart and use `try_strptime` with multiple patterns when doing date-based analysis (e.g. expirations).

### Tests and data quality

I added a mix of automatic checks (tests) to make sure the final tables look the way I expect and don’t quietly drift into bad data.

**Schema tests (`models/marts/schema.yml`):**

These are basic “shape of the table” checks:

- `licenses.id`  
  - Must always be present (`not_null`)  
  - Must be unique (no two rows share the same ID).

- `licenses.license_status`  
  - Must be one of a small list of allowed values: `Active`, `Inactive`, `Expired`, `Suspended`, or `Unknown`.

- `practitioners.id`  
  - Must always be present (`not_null`)  
  - Must be unique (one row per practitioner).

**Intermediate tests:**

These checks live on the models that sit between raw data and the final tables:

- `int_licenses_all.state_license_id`  
  - Must always be present. I use this field to track potential duplicate licenses coming from the raw files.

- `int_practitioners_all.practitioner_id`  
  - Must always be present and unique, so I know I really have one row per practitioner at that stage.

**Custom data tests:**

These are more “business logic” checks that don’t fit neatly in simple YAML:

- **Practitioner consistency**  
  I recompute practitioner IDs from the `licenses` table and check that they line up with the IDs in the `practitioners` table. This makes sure the two marts agree on “who” the practitioners are.

- **Active licenses have expirations**  
  I check that any license marked as `Active` actually has an `expiration_date` filled in. If an active license has no expiration date, the test fails.

Overall, these tests are there to catch problems like missing IDs, bad status values, or mismatches between tables early, instead of letting them leak into downstream analysis.

---

## Answers

> **How many Registered Nurses are there?**

Using the `practitioners` mart and looking for practitioners whose `license_types` contains `REGISTERED NURSE`:

```sql
select count(*) as registered_nurses
from {{ ref('practitioners') }}
where upper(license_types) like '%REGISTERED NURSE%';

ANSWER: 137

> **What's the breakdown of license counts by state?**
Using the licenses mart and grouping by the normalized license_state:

select
  license_state,
  count(*) as license_count
from {{ ref('licenses') }}
group by license_state
order by license_state;

ANSWER(s): 
- CA: 222
- MA: 136
- WA: 167

> **How many licenses will have expired after 2025-12-31?**
I parsed expiration_date from the licenses mart using multiple possible formats and counted licenses expiring strictly after 2025-12-31:

with parsed as (
    select
        *,
        coalesce(
            try_strptime(expiration_date, '%m-%d-%Y'),
            try_strptime(expiration_date, '%Y-%m-%d %H:%M:%S'),
            try_strptime(expiration_date, '%Y-%m-%d'),
            try_strptime(expiration_date, '%Y%m%d')
        )::date as expiration_date_parsed
    from {{ ref('licenses') }}
)
select count(*) as licenses_expiring_after_2025_12_31
from parsed
where expiration_date_parsed > date '2025-12-31';

Licenses expiring after 2025-12-31: 187