with
    payments as (
        select *
        from {{ source('erp', 'payment') }}
    )

    , transforms as (
        select
            cast(payment_id as int) as payment_id
            , cast(customer_id as int) as customer_id
            , cast(staff_id as int) as staff_id
            , cast(rental_id as int) as rental_id
            , cast(amount as numeric) as amount
            , cast(left(cast(payment_date as string), 10) as date) as payment_date
        from payments
    )
select *
from transforms