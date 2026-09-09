{{
    config(materialized='table')
}}

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('dwh_start_date') ~ "' as date)",
        end_date="cast(current_date as date) + interval '1 year'"
    ) }}
),

final as (
    select
        cast(to_char(date_day, 'YYYYMMDD') as int)  as date_key,
        cast(date_day as date)                      as calendar_date,
        extract(year from date_day)::int             as year,
        extract(quarter from date_day)::int           as quarter,
        extract(month from date_day)::int              as month,
        to_char(date_day, 'Month')                     as month_name,
        extract(day from date_day)::int                 as day_of_month,
        extract(dow from date_day)::int                  as day_of_week,
        to_char(date_day, 'Day')                          as day_name,
        extract(isodow from date_day) in (6, 7)            as is_weekend,
        extract(week from date_day)::int                    as iso_week
    from spine
)

select * from final