from os import path
from io import BufferedWriter
from typing import Union, Iterable
from datetime import timedelta
from requests import Response
from airflow.sdk import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.kafka.hooks.base import KafkaBaseHook
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.readwriter import DataFrameWriterV2
from pyspark.sql.connect.streaming.query import StreamingQuery
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.functions import col, substring, length
from pyspark.sql.connect.avro.functions import from_avro
from pyspark.sql.connect.functions.partitioning import months
from botocore.config import Config
from botocore.client import BaseClient
from settings import LOCAL_STORAGE_PATH, Connections

ColumnOrName = Union[Column, str]


@task(
	retries=3,
	retry_delay=timedelta(minutes=1),
	retry_exponential_backoff=True,
	execution_timeout=timedelta(minutes=15)
)
def download_file_to_local(
	http_conn_id: str,
	endpoint: str,
	save_to: str,
	method: str = 'GET'
) -> str:

	save_full_path: str = path.join(LOCAL_STORAGE_PATH,save_to)
	http_hook: HttpHook = HttpHook(http_conn_id=http_conn_id,method=method)
	response: Response
	file: BufferedWriter
	with (
		open(save_full_path, 'wb') as file,
		http_hook.run(endpoint=endpoint, extra_options={ 'stream': True }) as response
	):
		response.raise_for_status()
		for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
			file.write(chunk)
	return save_full_path


@task(
	retries=3,
	retry_delay=timedelta(minutes=1),
	retry_exponential_backoff=True,
	execution_timeout=timedelta(minutes=15)
)
def download_file_to_s3(
	http_conn_id: str,
	endpoint: str,
	aws_conn_id: str,
	bucket: str,
	s3_key: str,
	stream: bool = True,
	decode_from: str = None
) -> str:

	if stream and decode_from:
		raise NotImplementedError('Декодирование в потоковом формате не поддерживается.')

	s3_hook: S3Hook = S3Hook(aws_conn_id,config=Config(s3={'addressing_style': 'path' }))
	s3_client: BaseClient = s3_hook.get_conn()
	http_hook: HttpHook = HttpHook(http_conn_id=http_conn_id,method='GET')
	if stream:
		response: Response
		with http_hook.run(endpoint=endpoint, extra_options={ 'stream': True }) as response:
			response.raise_for_status()
			s3_client.upload_fileobj(response.raw,bucket,s3_key)
	else:
		response: Response = http_hook.run(endpoint=endpoint)
		body: bytes = response.content.decode(decode_from, errors='replace').encode() \
			if decode_from else response.content

		s3_client.put_object(
			Bucket=bucket,
			Key=s3_key,
			Body=body
		)

	return f's3a://{bucket}/{s3_key}'


@task(
	retries=3,
	retry_delay=timedelta(minutes=1),
	retry_exponential_backoff=True,
	execution_timeout=timedelta(minutes=15)
)
def get_latest_avro_schema(registry_conn_id: str, subject: str) -> str:

	registry_hook: HttpHook = HttpHook(http_conn_id=registry_conn_id, method='GET')
	response: Response = registry_hook.run(f'/subjects/{subject}/versions/latest')
	response.raise_for_status()
	return response.json()['schema']


@task.pyspark(conn_id=Connections.SPARK)
def kafka_to_iceberg(
	kafka_conn_id: str,
	avro_schema: str,
	topic: str,
	iceberg_table: str,
	spark: SparkSession
) -> str:

	kafka_bootstrap: str = (KafkaBaseHook
		.get_connection(kafka_conn_id)
		.extra_dejson
		.get('bootstrap.servers'))
	df: DataFrame = (spark.readStream
		.format('kafka')
		.option('kafka.bootstrap.servers', kafka_bootstrap)
		.option('subscribe', topic)
		.option('startingOffsets', 'earliest')
		.load())
	parsed_df: DataFrame = (df
		.withColumn('payload', substring('value',6,length('value')-5))
		.select(
			from_avro(col('payload'),avro_schema).alias('data'),
			col('timestamp').alias('kafka_ts'),
			col('partition').alias('kafka_partition'),
			col('offset').alias('kafka_offset')
		).select('data.*','kafka_ts','kafka_partition','kafka_offset'))
	query: StreamingQuery = (parsed_df.writeStream
		.format('iceberg')
		.outputMode('append')
		.option('checkpointLocation', f's3a://warehouse/checkpoints/{topic}')
		.trigger(availableNow=True)
		.toTable(iceberg_table))
	query.awaitTermination()
	return iceberg_table

