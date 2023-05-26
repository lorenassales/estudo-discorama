with
    categories as (
        select *
        from {{ source('erp', 'category') }}
    )

    , transforms as (
        select
            cast(category_id as int) as category_id
            , cast(name as string) as category_name
        from categories
    )
select *
from transforms