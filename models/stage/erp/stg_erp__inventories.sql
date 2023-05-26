with
    inventories as (
        select *
        from {{ source('erp', 'inventory') }}
    )

    , transforms as (
        select
            cast(inventory_id as int) as inventory_id
            , cast(film_id as int) as film_id
            , cast(store_id as int) as store_id
        from inventories
    )
select *
from transforms