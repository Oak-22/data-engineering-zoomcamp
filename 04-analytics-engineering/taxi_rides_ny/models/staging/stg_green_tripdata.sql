{{ config(materialized='view') }}   -- or 'table', depending on your preference

WITH tripdata AS (
    SELECT
        *,
        -- partition by vendorid & pickup time to detect duplicates
        row_number() OVER (PARTITION BY vendorid, lpep_pickup_datetime) AS rn
    FROM {{ source('staging','green_tripdata') }}
    WHERE vendorid IS NOT NULL
)

SELECT
    -- surrogate key from vendorid + pickup time
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} AS tripid,
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} AS vendorid,
    
    -- timestamps
    CAST(lpep_pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(lpep_dropoff_datetime AS timestamp) AS dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    CAST(trip_distance AS numeric) AS trip_distance,
    {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,

    -- payment info
    CAST(fare_amount as numeric) as fare_amount,
    CAST(extra as numeric) as extra,
    CAST(mta_tax as numeric) as mta_tax,
    CAST(tip_amount as numeric) as tip_amount,
    CAST(tolls_amount as numeric) as tolls_amount,
    CAST(ehail_fee as numeric) as ehail_fee,
    CAST(improvement_surcharge as numeric) as improvement_surcharge,
    CAST(total_amount as numeric) as total_amount,
    CAST(congestion_surcharge as numeric) AS congestion_surcharge,
    {{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }} as payment_type,

    -- Ratecode ID handling
    CASE 
      WHEN ratecodeid IN ('1','2','3','4','5','6','7','8','9') THEN ratecodeid::INT
      ELSE NULL
    END AS ratecodeid,

        -- Pickup location handling
    CASE
      WHEN pulocationid ~ '^\d+$' THEN pulocationid::INT
      ELSE NULL
    END AS pickup_locationid,

    -- Dropoff location handling
    CASE
      WHEN dolocationid ~ '^\d+$' THEN dolocationid::INT
      ELSE NULL 
    END AS dropoff_locationid,

    -- Using the macro 'get_payment_type)description.sql' to set the payment type description
    {{ get_payment_type_description(payment_type) }} AS payment_type_description

FROM tripdata
WHERE rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

limit 100

{% endif %}