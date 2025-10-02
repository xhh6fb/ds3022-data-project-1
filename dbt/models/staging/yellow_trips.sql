{{ config(materialized='table') }}

with factors as (
  select
    avg(case when lower(vehicle_type) like '%yellow%' then co2_grams_per_mile end) as y_gpm,
    avg(co2_grams_per_mile) as default_gpm
  from {{ source('nyc_taxi', 'vehicle_emissions') }}
)

select
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pu_location_id,
  do_location_id,
  total_amount,
  'yellow' as cab_color,

  round(trip_distance * coalesce((select y_gpm from factors), (select default_gpm from factors)) / 1000.0, 6) as trip_co2_kgs,

  case
    when (epoch(dropoff_datetime) - epoch(pickup_datetime)) > 0
      then trip_distance / ((epoch(dropoff_datetime) - epoch(pickup_datetime)) / 3600.0)
  end as avg_mph,

  date_part('hour', pickup_datetime) as hour_of_day,
  dayofweek(pickup_datetime) as day_of_week,
  date_part('week', pickup_datetime) as week_of_year,
  month(pickup_datetime) as month_of_year

from {{ source('nyc_taxi', 'yellow_clean') }}
where pickup_datetime >= timestamp '2015-01-01'
  and pickup_datetime <  timestamp '2025-01-01'
