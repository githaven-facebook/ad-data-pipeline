FROM apache/airflow:2.8.1-python3.10

LABEL maintainer="data-platform@fb.com"
LABEL description="Ad Data Pipeline Airflow image with custom plugins and dependencies"

# Switch to root for system package installation
USER root

# Install system dependencies for PySpark and pyarrow
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to airflow user for Python installs
USER airflow

# Copy dependency specification first for layer caching
COPY --chown=airflow:root pyproject.toml setup.cfg ./

# Install Python dependencies (production only - no dev tools)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        apache-airflow==2.8.1 \
        apache-airflow-providers-amazon>=8.14.0 \
        apache-airflow-providers-apache-spark>=4.3.0 \
        boto3>=1.34.0 \
        botocore>=1.34.0 \
        pyarrow>=14.0.2 \
        pandas>=2.1.4 \
        pyspark>=3.5.0 \
        pydantic>=2.5.3 \
        pydantic-settings>=2.1.0 \
        redis>=5.0.1 \
        sqlalchemy>=2.0.25 \
        statsd>=4.0.1 \
        requests>=2.31.0 \
        tenacity>=8.2.3

# Copy source library
COPY --chown=airflow:root src/ /opt/airflow/src/
RUN pip install --no-cache-dir -e /opt/airflow/src/../ 2>/dev/null || true

# Copy plugins directory into Airflow plugins path
COPY --chown=airflow:root plugins/ ${AIRFLOW_HOME}/plugins/

# Copy DAGs
COPY --chown=airflow:root dags/ ${AIRFLOW_HOME}/dags/

# Set PYTHONPATH to include src and plugins
ENV PYTHONPATH="${AIRFLOW_HOME}/plugins:${AIRFLOW_HOME}/src:${PYTHONPATH:-}"

# Airflow configuration via environment
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
ENV AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=5
ENV AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=INFO

# Healthcheck for the webserver
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080
