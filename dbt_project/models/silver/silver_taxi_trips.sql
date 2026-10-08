{{
	config(
		tblproperties={
			'write.sort.order': 'vendor_id, passenger_count, pickup_zone_id, dropoff_zone_id',
			'write.sort.mode': 'hierarchical'
		}
	)
}}

WITH raw_data AS (
	SELECT 
		CAST(VendorID AS INTEGER) AS vendor_id,
		CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_at,
		CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_at,
		CAST(passenger_count AS INTEGER) AS passenger_count,
		trip_distance,
		CAST(PULocationID AS INTEGER) AS pickup_zone_id,
		CAST(DOLocationID AS INTEGER) AS dropoff_zone_id,
		fare_amount,
		tip_amount,
		total_amount
	FROM {{ source('bronze', 'yellow_tripdata') }}
	WHERE tpep_pickup_datetime IS NOT NULL
)

SELECT * 
FROM raw_data
WHERE
	trip_distance > 0 
	AND passenger_count BETWEEN 1 AND 9
	AND total_amount > 0

