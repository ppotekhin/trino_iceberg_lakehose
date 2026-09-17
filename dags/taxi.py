from os import path, environ
from datetime import datetime
from enum import StrEnum
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.functions import date_trunc
from pyspark.sql.connect.functions.partitioning import months
from airflow.sdk import dag, task_group, task
from airflow.providers.http.hooks.http import HttpHook
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles.trino import TrinoBaseProfileMapping


DBT_PROJECT_PATH: str = path.join(environ.get('AIRFLOW_HOME'),'dbt_project')
LOCAL_STORAGE_PATH: str = '/home/iceberg/data'


class Connections(StrEnum):
	SPARK='spark-local'
	LAKEHOUSE='lakehouse'
	NYC_GOV='nyc-gov'


class NYC_ENDPOINTS(StrEnum):
	ZONE_LOOKUP='/misc/taxi_zone_lookup.csv'
	YELLOW_TRIPDATA='/trip-data/yellow_tripdata_{yyyy_mm}.parquet'


@task
def download_file_to_local(
	http_conn_id: str,
	endpoint: str,
	save_to: str,
	method: str = 'GET'
) -> str:

	save_full_path: str = path.join(LOCAL_STORAGE_PATH,save_to)
	http_hook: HttpHook = HttpHook(http_conn_id=http_conn_id,method=method)
	resp = http_hook.run(endpoint=endpoint, extra_options={ 'stream': True })
	resp.raise_for_status()
	with open(save_full_path, 'wb') as f:
		for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
			f.write(chunk)
	return save_full_path


@dag(
	start_date=datetime(2026, 9, 1),
	schedule=None,
	catchup=False,
	tags=['nyc-tlc']
)
def nyc_tlc():

	@task.pyspark(conn_id=Connections.SPARK)
	def create_lakehose(spark: SparkSession):
		spark.sql(f'CREATE DATABASE IF NOT EXISTS icbrg.bronze')
		spark.sql(f'CREATE DATABASE IF NOT EXISTS icbrg.silver')
		spark.sql(f'CREATE DATABASE IF NOT EXISTS icbrg.gold')

	@task.pyspark(conn_id=Connections.SPARK)
	def yellow_tripdata_to_iceberg(spark: SparkSession):

		full_table: str = 'icbrg.bronze.yellow_tripdata'
		src: str = 'file:///home/iceberg/data/yellow_tripdata_*.parquet'

		df: DataFrame = (spark.read.parquet(src)
			.withColumn('tpep_pickup_date', date_trunc('day','tpep_pickup_datetime')))
		(df
			.repartition(8,'tpep_pickup_date')
			.sortWithinPartitions('tpep_pickup_date','PULocationID','DOLocationID','passenger_count')
			.writeTo(full_table)
			.createOrReplace())

		return full_table

	@task.pyspark(conn_id=Connections.SPARK)
	def taxi_zone_to_iceberg(src: str, spark: SparkSession):

		full_table: str = 'icbrg.bronze.taxi_zone'
		src = f'file://{src}'

		df: DataFrame = spark.read.csv(src, header=True)
		(df
			.sortWithinPartitions('service_zone','Borough')
			.writeTo(full_table)
			.createOrReplace())

		return full_table

	dbt_transformations: DbtTaskGroup = DbtTaskGroup(
		group_id='dbt_transformations',
		project_config=ProjectConfig(DBT_PROJECT_PATH),
		execution_config=ExecutionConfig(
			dbt_executable_path=path.join(environ.get('AIRFLOW_HOME'),'dbt_venv/bin/dbt')
		),
		profile_config=ProfileConfig(
			profile_name='lakehouse',
			target_name='dev',
			profile_mapping=TrinoBaseProfileMapping(
				conn_id=Connections.LAKEHOUSE,
				profile_args={
					'database': 'icbrg',
					'schema': 'silver',
					'http_scheme': 'http',
					'threads': 1
				}
			)
		)
	)

	@task_group
	def taxi_zone_tg():

		taxi_zone_path = download_file_to_local(Connections.NYC_GOV,NYC_ENDPOINTS.ZONE_LOOKUP,'taxi_zone_lookup.csv')
		taxi_zone_to_iceberg(taxi_zone_path)

	@task_group
	def yellow_tripdata_tg():

		year: str = '2025'
		[
			download_file_to_local(
				http_conn_id=Connections.NYC_GOV,
				endpoint=NYC_ENDPOINTS.YELLOW_TRIPDATA.format(yyyy_mm=f'{year}-{month if month >= 10 else f"0{month}"}'),
				save_to=f'yellow_tripdata_{year}-{month if month >= 10 else f"0{month}"}.parquet'
			)
			for month in range(1,13)
		] >> yellow_tripdata_to_iceberg()

	create_lakehose() >> [yellow_tripdata_tg(),taxi_zone_tg()] >> dbt_transformations


nyc_tlc()

