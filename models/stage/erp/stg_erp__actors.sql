with
    actors as (
        select *
        from {{ source('erp', 'actor') }}
    )

    , transforms as (
        select
            cast(actor_id as int) as actor_id
            , cast((first_name || ' ' || last_name) as string) as actor_name            
        from actors
    )
select *
from transforms