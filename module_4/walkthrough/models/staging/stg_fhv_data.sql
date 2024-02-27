with 

source as (

    select
        *
    from {{ source('staging', 'fhv_data') }}

),

renamed as (

    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number
    from source

)

select {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime', 'dropoff_datetime']) }} as tripid,
* 
from renamed
where EXTRACT(year from date(pickup_datetime)) = 2019
