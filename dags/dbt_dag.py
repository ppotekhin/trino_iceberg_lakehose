from os import path, environ
from datetime import datetime
from enum import StrEnum
from cosmos import DbtDag, ProfileConfig, ProjectConfig
from cosmos.profiles import SparkThriftProfileMapping


DBT_PROJECT_PATH: str = path.join(environ.get('AIRFLOW_HOME'),'dbt_project')


class SparkSessionProfileMapping(SparkThriftProfileMapping):
	dbt_profile_method: str = 'session'


class Connections(StrEnum):
	SPARK='spark-local'
	LAKEHOUSE='lakehouse'
	NYC_GOV='nyc-gov'


dbt_transformations: DbtDag = DbtDag(
	start_date=datetime(2026, 9, 1),
	schedule=None,
	catchup=False,
	tags=['nyc-tlc'],
	dag_id='dbt_transformations',
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

dbt_transformations