with
    dim_customers as (
        select *
        from {{ ref('dim_customers') }}
    )

    , dim_staffs as (
        select *
        from {{ ref('dim_staffs') }}
    )

    , inventories as (
        select *
        from {{ ref('inventories') }}
    )

    , dim_films as (
        select *
        from {{ ref('dim_films') }}
    )

    , int_rentals_payments as (
        select *
        from {{ ref('int_rentals_payments') }}
    )

    , fct_rentals as (
        select
            rp.rental_id
            , rp.payment_id
            , c.store_id   
            , c.customer_sk as customer_fk                     
            , s.staff_sk as staff_fk            
            , i.inventory_sk as inventory_fk
            , f.film_sk as film_fk
            , c.customer_name
            , rp.rental_date
            , rp.return_date
            , rp.qty_rental_days
            , f.max_rental_duration
            , c.city
            , c.country
            , s.staff_name
            , f.title
            , f.category_name
            , f.rating            
            , f.rental_rate
            , f.replacement_cost
            , rp.amount
        from int_rentals_payments rp
        left join dim_customers c on
            rp.customer_id = c.customer_id        
        left join dim_staffs s on
            rp.staff_id = s.staff_id
        left join inventories i on
            rp.inventory_id = i.inventory_id
        left join dim_films f on
            i.film_id = f.film_id
    )
select 
    {{ dbt_utils.generate_surrogate_key(['rental_id', 'payment_id']) }} as rental_sk
    , *
from fct_rentals