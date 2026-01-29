-- dbt snapshots for customer SCD2
{{ config(
    materialized='table',
    schema='dwh_e-com',
    tags=['dimension'
) }}

with dates as (
    select
        generate_series(
            date '2018-01-01',
            date '2030-12-31',
            interval '1 day'
        )::date as date_day
)
select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_sk,
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    to_char(date_day, 'Day') as day_name,
    to_char(date_day, 'Month') as month_name,
    case 
       when extract(dow from date_day) in (0,6) then true 
       else false 
    end as is_weekend
from dates
