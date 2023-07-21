with
    stg_erp__staffs as (
        select *
        from {{ ref('stg_erp__staffs') }}
    )
select
    {{ dbt_utils.generate_surrogate_key(['staff_id']) }} as staff_sk
    , *
from stg_erp__staffs