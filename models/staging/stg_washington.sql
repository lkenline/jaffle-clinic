with

source as (

    select * from {{ source('licenses', 'washington') }}

)

select * from source
