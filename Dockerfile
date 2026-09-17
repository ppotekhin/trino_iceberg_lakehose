FROM apache/airflow:slim-3.3.0

USER root

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER airflow

COPY ./requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY ./dbt_requirements.txt /
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /dbt_requirements.txt

COPY --chown=airflow:root ./dbt_project /opt/airflow/dbt_project
RUN /opt/airflow/dbt_venv/bin/dbt deps --project-dir /opt/airflow/dbt_project

CMD [ "standalone" ]