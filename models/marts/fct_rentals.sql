with
    dim_customers as (
        select *
        from {{ ref('dim_customers') }}
    )

    , dim_staffs as (
        select *
        from {{ ref('dim_staffs') }}
    )

    , dim_inventories as (
        select *
        from {{ ref('dim_inventories') }}
    )

    , dim_films as (
        select *
        from {{ ref('dim_films') }}
    )

    , dim_film_actors as (
        select *
        from {{ ref('dim_film_actors') }}
    )

    , int_rentals_payments as (
        select *
        from {{ ref('int_rentals_payments') }}
    )

    , fct_rentals as (
        select
            rp.rental_id -- rental_id 4591: there are 5 line with this pk, because there are 5 payment_id diferents for the same customer and same film. Customer paid your rental in 5x.
            , rp.payment_id
            , c.customer_sk as customer_fk                     
            , s.staff_sk as staff_fk            
            , i.inventory_sk as inventory_fk
            , f.film_sk as film_fk
            , fa.film_actor_sk as film_actor_fk           
            , rp.rental_date
            , rp.return_date
            , rp.payment_date           
            , case
                when qty_rental_days > max_rental_duration then "Return late"
                when return_date is null then "Didn't return"
            else "Return on time"
            end as return_status
            , c.customer_name                    
            , s.staff_name
            , f.title
            , f.category_name
            , rp.amount
            , case
                when amount = 0 then "Not paid"                
                when amount >= rental_rate and return_date is null and amount < f.replacement_cost then "Missing replacement cost"                
            else "Paid"
            end as payment_status            
        from int_rentals_payments rp
        left join dim_customers c on
            rp.customer_id = c.customer_id        
        left join dim_staffs s on
            rp.staff_id = s.staff_id
        left join dim_inventories i on
            rp.inventory_id = i.inventory_id
        left join dim_films f on
            i.film_id = f.film_id
        left join dim_film_actors fa on
            i.film_id = fa.film_id
    )
select 
    {{ dbt_utils.generate_surrogate_key(['rental_id', 'payment_id']) }} as rental_sk
    , *    
from fct_rentals