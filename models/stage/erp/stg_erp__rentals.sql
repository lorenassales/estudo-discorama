with
    rentals as (
        select *
        from {{ source('erp', 'rental') }}
    )

    , transforms_01 as (
        select
            cast(rental_id as int) as rental_id
            , cast(staff_id as int) as staff_id            
            , cast(inventory_id as int) as inventory_id
            , cast(customer_id as int) as customer_id
            , cast(left(cast(rental_date as string), 10) as date) as rental_date
            , cast(left(cast(return_date as string), 10) as date) as return_date          
        from rentals
    )

    , transforms_02 as (
        select 
            *
            , cast(date_diff(return_date, rental_date, day) as int) as qty_rental_days 
        from transforms_01
    )
select
    *
from transforms_02
