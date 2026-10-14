from json import dumps
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from settings import Connections
from pyarrow import Table
from pyarrow.parquet import ParquetFile
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from settings import NYC_ENDPOINTS
from tasks import download_file_to_local


FILE_ENDPOINT: str = NYC_ENDPOINTS.YELLOW_TRIPDATA.format(yyyy_mm='2025-01')
FILE_SAVED_NAME: str = 'yellow_tripdata_2025-01.parquet'
TOPIC: str = 'yellow_tripdata_raw'
TOPIC_KEY: str = 'VendorID'
POLL_FREQ: int = 10_000
SCHEMA_REGISTRY_URL: str = 'http://schema-registry:8081'
AVRO_SCHEMA: dict = {
	'type': 'record',
	'name': 'YellowTripData',
	'namespace': 'com.nyc.taxi',
	'fields': [
		{ 'name': 'VendorID', 'type': ['long']},
		{
			'name': 'tpep_pickup_datetime',
			'type': [
				'null',
				{ "type": "long", "logicalType": "timestamp-millis" }
			],
			'default': None 
		},
		{
			'name': 'tpep_dropoff_datetime',
			'type': [
				'null',
				{ "type": "long", "logicalType": "timestamp-millis" }
			],
			'default': None
		},
		{ 'name': 'passenger_count', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'trip_distance', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'RatecodeID', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'store_and_fwd_flag', 'type': ['null', 'string'], 'default': None },
		{ 'name': 'PULocationID', 'type': ['null', 'long'], 'default': None },
		{ 'name': 'DOLocationID', 'type': ['null', 'long'], 'default': None },
		{ 'name': 'payment_type', 'type': ['null', 'long'], 'default': None },
		{ 'name': 'fare_amount', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'extra', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'mta_tax', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'tip_amount', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'tolls_amount', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'improvement_surcharge', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'total_amount', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'congestion_surcharge', 'type': ['null', 'double'], 'default': None },
		{ 'name': 'airport_fee', 'type': ['null', 'double'], 'default': None },
		{
			'name': 'tpep_pickup_date',
			'type': [
				'null',
				{ "type": "long", "logicalType": "timestamp-millis" }
			],
			'default': None
		}
	]
}


@dag(
	start_date=datetime(2026, 9, 1),
	schedule=None,
	catchup=False,
	tags=['nyc-tlc']
)
def nyc_tlc_kafka_provider():

	@task
	def provide_yellow_tripdata(source_file_path: str):

		parquet_file: ParquetFile = ParquetFile(source_file_path)
		schema_registry_conf: dict = { 'url': SCHEMA_REGISTRY_URL }
		schema_registry_client: SchemaRegistryClient = SchemaRegistryClient(schema_registry_conf)
		avro_serializer: AvroSerializer = AvroSerializer(
			schema_registry_client,
			dumps(AVRO_SCHEMA)
		)
		string_serializer: StringSerializer = StringSerializer('utf_8')
		kafka_hook: KafkaProducerHook = KafkaProducerHook(Connections.KAFKA)

		with kafka_hook.get_producer() as producer:
			for i in range(parquet_file.num_row_groups):
				row_group: Table = parquet_file.read_row_group(i)
				for j, record_dict in enumerate(row_group.to_pylist()):

					key_bytes: bytes = string_serializer(str(record_dict[TOPIC_KEY]))
					value_bytes: bytes = avro_serializer(
						record_dict,
						SerializationContext(TOPIC, MessageField.VALUE)
					)
					producer.produce(
						topic=TOPIC,
						key=key_bytes,
						value=value_bytes
					)
					if j%POLL_FREQ==0:
						producer.poll(0)

	source_file_path: str = download_file_to_local(
		http_conn_id=Connections.NYC_GOV,
		endpoint=FILE_ENDPOINT,
		save_to=FILE_SAVED_NAME
	)
	provide_yellow_tripdata(source_file_path)


nyc_tlc_kafka_provider()

