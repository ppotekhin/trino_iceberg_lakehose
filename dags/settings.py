from os import path, environ
from enum import StrEnum
from cosmos.profiles import SparkThriftProfileMapping


DBT_PROJECT_PATH: str = path.join(environ.get('AIRFLOW_HOME'),'dbt_project')
LOCAL_STORAGE_PATH: str = '/home/iceberg/data'
WAREHOUSE_BUCKET: str = 'warehouse'


class SparkSessionProfileMapping(SparkThriftProfileMapping):
	dbt_profile_method: str = 'session'


class Connections(StrEnum):
	SPARK='spark-local'
	LAKEHOUSE='lakehouse'
	NYC_GOV='nyc-gov'
	KAFKA='kafka'
	AVRO_REGISTRY='avro_reg'


class NYC_ENDPOINTS(StrEnum):
	ZONE_LOOKUP='/misc/taxi_zone_lookup.csv'
	YELLOW_TRIPDATA='/trip-data/yellow_tripdata_{yyyy_mm}.parquet'

