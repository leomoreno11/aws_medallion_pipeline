from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from slack_shared_functions import slack_notifier
from s3_shared_functions import read_s3_object
from open_brewery_db_functions import api_data_load_to_s3, validate_insert
from airflow.models import Variable
from datetime import datetime
from airflow import DAG
import pendulum
from logging_config import logger

local_tz = pendulum.timezone(Variable.get("br_timezone"))

default_args = {
    "owner": "raven",
    "depends_on_past": False,
    "on_failure_callback": slack_notifier,
    "start_date": datetime(2024, 10, 17, tzinfo=local_tz),
}

dag = DAG(
    dag_id="OPEN_BREWERY_DB_DATA_PIPELINE",
    catchup=False,
    default_args=default_args,
    schedule_interval="0 0 * * *",
    tags=["Open Brewery DB"],
    doc_md="""
#### Description
This DAG extracts data from the Open Brewery DB API and loads it into a data lake
following a layered architecture: Bronze (raw), Silver (cleaned), and Gold (aggregated).

- **Schedule**: Daily at midnight (`0 0 * * *`)
- **Catchup**: Disabled
- **Data Quality Checks**: Ensures accuracy at each stage with validation.
- **Notifications**: Slack alerts on failures.
    """,
)

with dag:
    airflow_s3_bucket = Variable.get("airflow_s3_bucket")
    open_brewery_db_query_path = Variable.get("open_brewery_db_query_path")
    snowflake_database = Variable.get("snowflake_database")
    snowflake_conn = "snowflake_conn"

    START_WORKFLOW = DummyOperator(task_id="start_workflow")    
    END_WORKFLOW = DummyOperator(task_id="end_workflow")
    STAGE_I = DummyOperator(task_id="stage_i")
    STAGE_II = DummyOperator(task_id="stage_ii")
    
    logger.info(f"DEBUG: Variable open_brewery_db_api_response_path = {Variable.get('open_brewery_db_api_response_path')}")
    RAW_DATA_LOAD_TO_S3 = PythonOperator(
        task_id="raw_data_load_to_s3",
        python_callable=api_data_load_to_s3,
        op_kwargs={
            'url': Variable.get("open_brewery_db_url"),
            'endpoint': 'breweries',
            's3_bucket': airflow_s3_bucket,
            's3_target_path': Variable.get("open_brewery_db_api_response_path"),  # Aqui garantimos que é do Airflow
            'file_name': 'open_brewery_db_response',
            'ext': 'json'        
        },
    )

    BRONZE_INSERTION_VALIDATION = PythonOperator(
        task_id='validate_bronze_insertion',
        python_callable=validate_insert,
        op_kwargs={'task_id': 'raw_data_load_to_data_lake_bronze_layer', 'min_rows': 1},
    )

    SILVER_INSERTION_VALIDATION = PythonOperator(
        task_id='validate_silver_insertion',
        python_callable=validate_insert,
        op_kwargs={'task_id': 'data_transformation_to_data_lake_silver_layer', 'min_rows': 1},
    )

    def create_snowflake_task(task_id, sql_file):
        return SnowflakeOperator(
            task_id=task_id,
            sql=read_s3_object(
                s3_bucket_name=airflow_s3_bucket,
                s3_object_path=open_brewery_db_query_path,
                object_file_name=sql_file,
            ),
            params={"database_name": snowflake_database},
            snowflake_conn_id=snowflake_conn,
            do_xcom_push=True if "silver" in task_id or "bronze" in task_id else False
        )

    RAW_DATA_LOAD_TO_DATA_LAKE_BRONZE_LAYER = create_snowflake_task(
        "raw_data_load_to_data_lake_bronze_layer", "OPEN_BREWERY_DB_BRONZE_LAYER_INSERTION.sql"
    )
    
    DATA_TRANSFORMATION_TO_DATA_LAKE_SILVER_LAYER = create_snowflake_task(
        "data_transformation_to_data_lake_silver_layer", "OPEN_BREWERY_DB_SILVER_LAYER_INSERTION.sql"
    )

    AGG_VIEW_GENERATION_TO_DATA_LAKE_GOLD_LAYER = create_snowflake_task(
        "agg_view_generation_to_data_lake_gold_layer", "OPEN_BREWERY_DB_GOLD_LAYER_VIEW_GENERATION.sql"
    )

    START_WORKFLOW >> RAW_DATA_LOAD_TO_S3 >> RAW_DATA_LOAD_TO_DATA_LAKE_BRONZE_LAYER >> BRONZE_INSERTION_VALIDATION >> STAGE_I
    STAGE_I >> DATA_TRANSFORMATION_TO_DATA_LAKE_SILVER_LAYER >> SILVER_INSERTION_VALIDATION >> STAGE_II
    STAGE_II >> AGG_VIEW_GENERATION_TO_DATA_LAKE_GOLD_LAYER >> END_WORKFLOW