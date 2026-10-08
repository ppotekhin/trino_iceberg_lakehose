{{
	config(
		tblproperties={
			'write.sort.order': 'pickup_borough, pickup_zone, date_hour',
			'write.sort.mode': 'hierarchical'
		}
	)
}}


WITH trips AS (
    SELECT * FROM {{ ref('silver_taxi_trips') }}
),

zones AS (
    SELECT * FROM {{ ref('silver_taxi_zones') }}
)

SELECT
    DATE_TRUNC('hour', t.pickup_at) AS date_hour,
    z.borough AS pickup_borough,
    z.zone_name AS pickup_zone,
    CAST(COUNT(*) AS INTEGER) AS total_trips,
    CAST(SUM(t.passenger_count) AS INTEGER) AS total_passengers,
    ROUND(AVG(t.trip_distance), 2) AS avg_distance,
    ROUND(SUM(t.total_amount), 2) AS total_revenue,
    ROUND(AVG(t.tip_amount / NULLIF(t.fare_amount, 0)) * 100, 2) AS avg_tip_percent
FROM trips t
JOIN zones z ON t.pickup_zone_id = z.zone_id
GROUP BY 1, 2, 3