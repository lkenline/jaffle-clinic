with

source as (

    select * from {{ source('licenses', 'misc') }}

)

select * from source
