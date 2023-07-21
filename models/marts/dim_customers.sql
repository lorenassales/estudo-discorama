with
    stg_erp__addresses as (
        select *
        from {{ ref('stg_erp__addresses') }}
    )

    , stg_erp__cities as (
        select *
        from {{ ref('stg_erp__cities') }}
    )

    , stg_erp__countries as (
        select *
        from {{ ref('stg_erp__countries') }}
    )

    , stg_erp__customers as (
        select *
        from {{ ref('stg_erp__customers') }}
    )

    , dim_customers as (
        select
            customers.customer_id
            , customers.store_id
            , addresses.address_id            
            , cities.city_id
            , countries.country_id            
            , customers.customer_name
            , addresses.address
            , cities.city
            , countries.country
            , customers.create_date
            , customers.is_active
        from stg_erp__customers customers
        left join stg_erp__addresses addresses on
            customers.address_id = addresses.address_id
        left join stg_erp__cities cities on
            addresses.city_id = cities.city_id
        left join stg_erp__countries countries on 
            cities.country_id = countries.country_id
    )
select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk
    , *
from dim_customers