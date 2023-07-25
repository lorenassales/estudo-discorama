with
    stg_erp__actors as (
        select *
        from {{ ref('stg_erp__actors') }}
    )
    
    , stg_erp__film_actors as (
        select *
        from {{ ref('stg_erp__film_actors') }}
    )

    , dim_film_actors as (
        select
            fa.film_id
            , a.actor_id
            , a.actor_name
        from stg_erp__film_actors fa
        left join stg_erp__actors a on
            fa.actor_id = a.actor_id
    )
select
    {{ dbt_utils.generate_surrogate_key(['film_id', 'actor_id']) }} as film_actor_sk
    , *
from dim_film_actors