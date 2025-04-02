FROM python:3.9-slim
WORKDIR /usr/src/app
COPY . .
CMD ["python", "./app.py"]
RUN apt-get update && apt-get install -y tzdata
RUN pip install apache-airflow-providers-snowflake

FROM apache/airflow:2.9.1

USER root

# Instala o AWS CLI
RUN apt-get update && apt-get install -y awscli

USER airflow