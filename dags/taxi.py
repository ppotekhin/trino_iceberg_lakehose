from datetime import datetime
from pyspark.sql.connect.session import SparkSession
from airflow.sdk import dag, task_group, task
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from settings import DBT_PROJECT_PATH, WAREHOUSE_BUCKET, Connections, SparkSessionProfileMapping, NYC_ENDPOINTS
from tasks import (
	download_file_to_s3,
	get_latest_avro_schema,
	kafka_to_iceberg
)


@dag(
	start_date=datetime(2026, 9, 1),
	schedule=None,
	catchup=False,
	tags=['nyc-tlc']
)
def nyc_tlc():

	@task.pyspark(conn_id=Connections.SPARK)
	def create_lakehose(spark: SparkSession) -> None:

		spark.sql('CREATE NAMESPACE IF NOT EXISTS icbrg.bronze')
		spark.sql('CREATE NAMESPACE IF NOT EXISTS icbrg.silver')
		spark.sql('CREATE NAMESPACE IF NOT EXISTS icbrg.gold')

	@task_group
	def yellow_tripdata_tg() -> None:

		@task.pyspark(conn_id=Connections.SPARK)
		def create_yellow_tripdata_table(spark: SparkSession) -> str:

			table_name: str = 'icbrg.bronze.yellow_tripdata'
			table_schema: str = '''
				VendorID long,
				tpep_pickup_datetime timestamp,
				tpep_dropoff_datetime timestamp,
				passenger_count double,
				trip_distance double,
				RatecodeID double,
				store_and_fwd_flag string,
				PULocationID long,
				DOLocationID long,
				payment_type long,
				fare_amount double,
				extra double,
				mta_tax double,
				tip_amount double,
				tolls_amount double,
				improvement_surcharge double,
				total_amount double,
				congestion_surcharge double,
				airport_fee double,
				tpep_pickup_date timestamp,
				kafka_ts timestamp,
				kafka_partition int,
				kafka_offset long
			'''

			spark.sql(f'''
				CREATE TABLE IF NOT EXISTS {table_name}({table_schema})
				USING iceberg
				PARTITIONED BY (months(kafka_ts))
			''')

			return table_name

		iceberg_table: str = create_yellow_tripdata_table()

		avro_schema: str = get_latest_avro_schema(
			Connections.AVRO_REGISTRY,
			'yellow_tripdata_raw-value'
		)

		kafka_to_iceberg(
			kafka_conn_id=Connections.KAFKA,
			avro_schema=avro_schema,
			topic='yellow_tripdata_raw',
			iceberg_table=iceberg_table
		)

	@task_group
	def taxi_zone_tg() -> None:

		@task.pyspark(conn_id=Connections.SPARK)
		def taxi_zone_to_iceberg(src: str, spark: SparkSession) -> str:

			full_table: str = 'icbrg.bronze.taxi_zone'
			(spark.read.csv(src,header=True,encoding='utf-8')
				.writeTo(full_table)
				.tableProperty('write.sort.order', 'service_zone, Borough')
				.tableProperty('write.sort.mode', 'partition')
				.createOrReplace())

			return full_table

		s3_path: str = download_file_to_s3(
			Connections.NYC_GOV,
			NYC_ENDPOINTS.ZONE_LOOKUP,
			Connections.LAKEHOUSE,
			WAREHOUSE_BUCKET,
			'landing/taxi_zone_lookup/{{ run_id }}.csv',
			stream=False,
			decode_from='windows-1251'
		)
		taxi_zone_to_iceberg(s3_path)

	dbt_transformations: DbtTaskGroup = DbtTaskGroup(
		group_id='dbt_transformations',
		project_config=ProjectConfig(DBT_PROJECT_PATH),
		profile_config=ProfileConfig(
			profile_name='lakehouse',
			target_name='dev',
			profile_mapping=SparkSessionProfileMapping(
				conn_id=Connections.SPARK,
				profile_args={ 'schema': 'silver' }
			)
		)
	)

	create_lakehose() >> [yellow_tripdata_tg(),taxi_zone_tg()] >> dbt_transformations


nyc_tlc()

