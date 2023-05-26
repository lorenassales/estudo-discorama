with
    rentals as (
        select *
        from {{ source('erp', 'rental') }}
    )

    , transforms as (
        select
            cast(rental_id as int) as rental_id
            , cast(staff_id as int) as staff_id            
            , cast(inventory_id as int) as inventory_id
            , cast(customer_id as int) as customer_id
            , cast(rental_date as datetime) as rental_date
            , cast(return_date as datetime) as return_date            
        from rentals
    )
select *
from transforms