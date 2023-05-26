with
    addresses as (
        select *
        from {{ source('erp', 'address') }}
    )

    , transforms as (
        select
            cast(address_id as int) as address_id
            , cast(city_id as int) as city_id
            , cast(address as string) as address            
            , cast(district as string) as district            
        from addresses
    )
select *
from transforms