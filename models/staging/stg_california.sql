with

source as (

    select * from {{ source('licenses', 'california') }}

)

select * from source
