with
    countries as (
        select *
        from {{ source('erp', 'country') }}
    )

    , transforms as (
        select
            cast(country_id as int) as country_id
            , cast(country as string) as country
        from countries
    )
select *
from transforms