import time
import json
import requests
from s3_shared_functions import upload_str_data_to_s3
from logging_config import logger

def fetch_api_data(url, endpoint):
    """
    Fetches data from a specified API endpoint.

    Parameters:
    - url (str): Base URL of the API.
    - endpoint (str): Specific API endpoint to append to the base URL.

    Returns:
    - dict: The JSON response parsed into a dictionary if the request is successful.

    Raises:
    - Exception: If the API request fails (non-200 status code), an exception is raised.
    """
    full_url = f"{url}{endpoint}"
    logger.info(f"Fetching API data from: {full_url}")
    response = requests.get(full_url)
    if response.status_code != 200:
        logger.error(f"API request failed: {response.status_code} - {response.text}")
        raise Exception(f"Failed to retrieve API data. Status code: {response.status_code}, Response: {response.text}")
    return response.json()

def api_data_load_to_s3(url, endpoint, s3_bucket, s3_target_path, file_name, ext, retries=3, delay=5):
    for attempt in range(retries):
        json_response = fetch_api_data(url, endpoint)
        logger.info(f"Attempt {attempt+1}: API Response: {json.dumps(json_response, indent=2)}")
        
        if json_response:  
            json_str_data = json.dumps(json_response)
            logger.info(f"Data being uploaded: {json_str_data}")
            logger.info(f"Uploading file: {file_name}.{ext} to {s3_bucket}/{s3_target_path}")
            upload_str_data_to_s3(json_str_data, s3_bucket, s3_target_path, file_name, ext)
            return
        
        logger.warning(f"Attempt {attempt+1}: API returned empty response, retrying in {delay} seconds")
        if attempt < retries - 1:
            time.sleep(delay)

    raise ValueError("No data retrieved from the API after multiple attempts, aborting upload to S3.")


def validate_insert(**kwargs):
    """
    Validates the number of rows inserted into a target database to ensure data quality.

    Parameters:
    kwargs (dict): Dictionary of keyword arguments. It must include:
        - task_id (str): Task ID for which the row insertion needs to be validated.
        - min_rows (int, optional): Minimum number of rows expected to be inserted (default is 1).
        - ti (TaskInstance): The Airflow TaskInstance object that allows pulling of XCom values.

    Raises:
    - ValueError: If the number of rows inserted is less than the expected minimum, an error is raised.
    """
    task_id = kwargs["task_id"]
    min_rows = kwargs.get("min_rows", 1)
    ti = kwargs["ti"]
    rows_inserted = ti.xcom_pull(task_ids=task_id)

    logger.info(f"Rows inserted for task {task_id}: {rows_inserted}")
    
    if not rows_inserted or rows_inserted[0].get("ROWS_INSERTED", 0) < min_rows:
        raise ValueError(f"Validation failed: less than {min_rows} rows inserted for task {task_id}.")
    
    logger.info(f"Validation passed: {rows_inserted[0]['ROWS_INSERTED']} rows inserted for task {task_id}.")
