{{
    config(
        severity = 'error'
    )
}}
with
    general_payments as (
        select
            sum(amount) as total_payment
        from {{ ref('fct_rentals') }}        
    )
select total_payment
from general_payments
where total_payment not between 61312.00 and 61313.00