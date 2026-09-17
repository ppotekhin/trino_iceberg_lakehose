{{ config(materialized='view') }}

SELECT
	CAST(LocationID AS INTEGER) AS zone_id,
	Borough AS borough,
	"Zone" AS zone_name,
	service_zone
FROM {{ source('bronze', 'taxi_zone') }}