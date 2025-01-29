{{ config(materialized='view') }}   -- or 'table', depending on your preference

WITH tripdata AS (
    SELECT
        *,
        -- partition by vendorid & pickup time to detect duplicates
        row_number() OVER (PARTITION BY vendorid, tpep_pickup_datetime) AS rn
    FROM {{ source('staging','yellow_tripdata') }}
    WHERE vendorid IS NOT NULL
)

-- Removed the initial safe_cast for ratecodeid and keep only the case statement version
SELECT
    -- Surrogate key from vendorid + pickup time
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} AS tripid,
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} AS vendorid,
    -- Safe casting for other fields
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} AS pickup_locationid,
    CAST(tpep_pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS timestamp) AS dropoff_datetime,
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} AS passenger_count,
    CAST(trip_distance AS numeric) AS trip_distance,
    1 AS trip_type,
    CAST(fare_amount AS numeric) AS fare_amount,
    CAST(extra AS numeric) AS extra,
    CAST(mta_tax AS numeric) AS mta_tax,
    CAST(tip_amount AS numeric) AS tip_amount,
    CAST(tolls_amount AS numeric) AS tolls_amount,
    CAST(0 AS numeric) AS ehail_fee,
    CAST(improvement_surcharge AS numeric) AS improvement_surcharge,
    CAST(total_amount AS numeric) AS total_amount,
    -- Ratecode ID handling
    CASE
      WHEN ratecodeid IN ('1','2','3','4','5','6','7','8','9') THEN ratecodeid::INT
      ELSE NULL
    END AS ratecodeid,
    {{ get_payment_type_description('payment_type') }} AS payment_type_description
FROM tripdata
WHERE rn = 1

{% if var('is_test_run', default=true) %}
LIMIT 100
{% endif %}
