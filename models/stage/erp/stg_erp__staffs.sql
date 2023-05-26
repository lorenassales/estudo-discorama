with
    staffs as (
        select *
        from {{ source('erp', 'staff') }}
    )

    , transforms as (
        select
            cast(staff_id as int) as staff_id
            , cast(store_id as int) as store_id
            , cast(address_id as int) as address_id
            , cast((first_name || ' ' || last_name) as string) as name
            , cast(active as boolean) as active 
        from staffs
    )
select *
from transforms
