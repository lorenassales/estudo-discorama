with
    film_actors as (
        select *
        from {{ source('erp', 'film_actor') }}
    )

    , transforms as (
        select
            cast(film_id as int) as film_id
            , cast(actor_id as int) as actor_id
        from film_actors
    )
select *
from transforms