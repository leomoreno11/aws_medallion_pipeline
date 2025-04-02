from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from logging_config import logger

def upload_str_data_to_s3(data: str, s3_bucket: str, s3_target_path: str, file_name: str, ext: str) -> None:
    s3_target_path = "airflow_stage/open_brewery_db/raw_api_responses/"
    file_key = f"airflow_stage/open_brewery_db/raw_api_responses/{file_name}.{ext}"
    s3_conn = S3Hook("s3_conn")
    # Adicionando logs para debug
    logger.info(f"DEBUG: s3_bucket = {s3_bucket}")
    logger.info(f"DEBUG: s3_target_path = {s3_target_path}")
    logger.info(f"DEBUG: file_name = {file_name}")
    logger.info(f"DEBUG: ext = {ext}")
    logger.info(f"DEBUG: Final file_key = {file_key}")

    try:
        logger.info(f"Connecting to S3: {s3_bucket}")
        existing_files = s3_conn.list_keys(bucket_name=s3_bucket)
        logger.info(f"Existing files in bucket: {existing_files}")

        logger.info(f"Uploading file to S3: s3://{s3_bucket}/{file_key}")
        s3_conn.load_string(string_data=data, key=file_key, bucket_name=s3_bucket, replace=True)
        logger.info(f"Upload successful: {file_key}")
    except Exception as e:
        logger.error(f"Failed to upload {file_key} to {s3_bucket}: {e}")
        raise

def read_s3_object(s3_bucket_name: str, s3_object_path: str, object_file_name: str) -> str:
    """
    Reads an object from an S3 bucket and returns its content as a string.
    """

    file_key = f"{s3_object_path}{object_file_name}"
    s3_conn = S3Hook("s3_conn")

    try:
        logger.info(f"Reading file from S3: s3://{s3_bucket_name}/{file_key}")
        s3_object = s3_conn.get_key(bucket_name=s3_bucket_name, key=file_key)
        content = s3_object.get()["Body"].read().decode("utf-8")
        return content
    except Exception as e:
        logger.error(f"Failed to read {file_key} from {s3_bucket_name}: {e}")
        raise