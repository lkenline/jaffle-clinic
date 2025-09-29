with

source as (

    select * from {{ source('licenses', 'massachusetts') }}

)

select * from source
