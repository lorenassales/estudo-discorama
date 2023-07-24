with
   stg_erp__inventories as (
        select *            
        from {{ ref('stg_erp__inventories') }}
    ) 

    , dim_inventories as (
        select
            inventory_id
            , film_id
            , store_id
            , count(*) over(partition by film_id, store_id) as qty_stock_by_store
        from stg_erp__inventories       
    )
select 
    {{ dbt_utils.generate_surrogate_key(['inventory_id']) }} as inventory_sk
    , *
from dim_inventories
