FROM apache/airflow:2.10.5-python3.11

USER root
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/pipeline-venv \
    && /opt/pipeline-venv/bin/pip install --upgrade pip \
    && /opt/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
    && chown -R airflow:0 /opt/pipeline-venv
USER airflow

ENV PROJECT_DIR=/opt/airflow/project \
    PIPELINE_PYTHON=/opt/pipeline-venv/bin/python \
    PIPELINE_DBT=/opt/pipeline-venv/bin/dbt \
    PYTHONPATH=/opt/airflow/project
