FROM apache/airflow:2.10.5-python3.11

USER root
COPY requirements.txt requirements-streaming.txt requirements-runtime.txt /tmp/
# Group 0 must be able to run this venv. Compose starts the container as
# ${AIRFLOW_UID:-50000}:0, which is not always the airflow user (uid 50000).
RUN python -m venv /opt/pipeline-venv \
    && /opt/pipeline-venv/bin/pip install --upgrade pip \
    && /opt/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
    && chown -R airflow:0 /opt/pipeline-venv \
    && chmod -R g=u /opt/pipeline-venv
USER airflow

ENV PROJECT_DIR=/opt/airflow/project \
    PIPELINE_PYTHON=/opt/pipeline-venv/bin/python \
    PIPELINE_DBT=/opt/pipeline-venv/bin/dbt \
    PYTHONPATH=/opt/airflow/project
