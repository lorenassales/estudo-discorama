with
    customers as (
        select *
        from {{ source('erp', 'customer') }}
    )

    , transforms as (
        select
            cast(customer_id as int) as customer_id
            , cast(store_id as int) as store_id
            , cast(address_id as int) as address_id
            , cast((first_name || ' ' || last_name) as string) as customer_name           
            , cast(create_date as date) as create_date
           -- , cast(last_update as string) as last_update
            , case 
                when active = 1 then "active"
                else "inactive"
                end as is_active

        from customers
    )
select *
from transforms