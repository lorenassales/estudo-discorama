with
    stg_erp__payments as (
        select *
        from {{ ref('stg_erp__payments') }}
    )

    , stg_erp__rentals as (
        select *
        from {{ ref('stg_erp__rentals') }}
    )

    , int_rentals_payments as (
        select
            r.rental_id
            , r.staff_id
            , r.inventory_id
            , r.customer_id
            , p.payment_id
            , r.rental_date
            , r.return_date
            , r.qty_rental_days
            , case
                when p.amount is null then 0
            else p.amount
            end as amount
        from stg_erp__rentals r
        left join stg_erp__payments p on
            r.rental_id = p.rental_id
    )
select *
from int_rentals_payments