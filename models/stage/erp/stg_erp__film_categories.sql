with
    film_categories as (
        select *
        from {{ source('erp', 'film_category') }}
    )

    , transforms as (
        select
            cast(film_id as int) as film_id
            , cast(category_id as int) as category_id
        from film_categories
    )
select * 
from transforms