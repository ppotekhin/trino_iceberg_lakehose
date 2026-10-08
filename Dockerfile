FROM apache/airflow:slim-3.3.0

USER root

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER airflow

COPY ./requirements.txt /
RUN pip install --no-cache-dir --pre -r /requirements.txt

# COPY ./requirements_no_deps.txt /
# RUN pip install --no-cache-dir --no-deps --pre -r /requirements_no_deps.txt

COPY --chown=airflow:root ./dbt_project /opt/airflow/dbt_project
RUN dbt deps --project-dir /opt/airflow/dbt_project

CMD [ "standalone" ]