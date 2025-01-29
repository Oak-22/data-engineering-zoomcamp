{{
    config(
        materialized='table'
    )
}}
with green_tripdata as (
    select 
        tripid,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        ratecodeid,
        pickup_locationid,
        dropoff_locationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type_description,
        trip_type,
        congestion_surcharge,                                                                                                            
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select 
        tripid,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pickup_locationid,
        dropoff_locationid,
        payment_type_description,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        CAST(NULL AS NUMERIC) as ehail_fee,  -- Added to match green_tripdata
        CAST(NULL AS INTEGER) as trip_type, -- Added to match green_tripdata
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select 
        tripid,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        ratecodeid,
        pickup_locationid,
        dropoff_locationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type_description,
        trip_type,
        congestion_surcharge,
        service_type
    from green_tripdata
    union all 
    select 
        tripid,
        vendorid,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        ratecodeid,
        pickup_locationid,
        dropoff_locationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type_description,
        trip_type,
        congestion_surcharge,
        service_type
    from yellow_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type_description,
    trips.unioned.congestion_surcharge
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid