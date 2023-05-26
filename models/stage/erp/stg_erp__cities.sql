with
    cities as (
        select *
        from {{ source('erp', 'city') }}
    )

    , transforms as (
        select
            cast(city_id as int) as city_id
            , cast(country_id as int) as country_id
            , cast(city as string) as city            
        from cities
    )
select *
from transforms