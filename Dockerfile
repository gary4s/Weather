FROM apache/airflow:2.7.3

USER root
# Install system dependencies needed for SQL Server drivers
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  gcc \
  python3-dev \
  freetds-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --user -r /requirements.txt