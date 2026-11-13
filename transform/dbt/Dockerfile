FROM python:3.12.10-slim
WORKDIR /project
RUN pip install --no-cache-dir dbt-core==1.8.3 dbt-clickhouse==1.8.1
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY . /project
ENV DBT_PROFILES_DIR=/project
CMD ["dbt", "--version"]
