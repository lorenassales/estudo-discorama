with
    stg_erp__film_categories as (
        select *
        from {{ ref('stg_erp__film_categories') }}
    )

    , stg_erp__categories as (
        select *
        from {{ ref('stg_erp__categories') }}
    )

    , stg_erp__inventories as (
        select count(film_id) as qty_stock, film_id
        from {{ ref('stg_erp__inventories') }}
        group by film_id
    ) -- Used for quantify and bring total inventory by films

    , stg_erp__films as (
        select *
        from {{ ref('stg_erp__films') }}
    )   

    , dim_films as (
        select
            f.film_id
            , c.category_id
            , f.title
            , c.category_name
            , f.release_year
            , f.max_rental_duration
            , f.rental_rate
            , f.replacement_cost
            , f.rating
            , i.qty_stock            
        from stg_erp__films f
        left join stg_erp__film_categories fc on 
            f.film_id = fc.film_id
        left join stg_erp__categories c on
            fc.category_id = c.category_id
        left join stg_erp__inventories i on
            f.film_id = i.film_id
    )
select 
    {{ dbt_utils.generate_surrogate_key(['film_id']) }} as film_sk
    , *
from dim_films
