with
    films as (
        select *
        from {{ source('erp', 'film') }}
    )

    , transforms as (
        select
            cast(film_id as int) as film_id
            , cast(title as string) as title             
            , cast(release_year as int) as release_year            
            , cast(rental_duration as int) as rental_duration
            , cast(rental_rate as numeric) as rental_rate             
            , cast(replacement_cost as numeric) as replacement_cost
            , cast(rating as string) as rating
        from films
    )
select *
from transforms