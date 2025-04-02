-- Ensure correct schema context
USE {{params.database_name}}.BRONZE;

-- Clear existing data
DELETE FROM RAW_OPEN_BREWERY_DB_DATA;

-- Load data efficiently with error validation
COPY INTO RAW_OPEN_BREWERY_DB_DATA
FROM @open_brewery_db_stg/open_brewery_db_response.json
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = RETURN_ERRORS;

-- Validate the number of inserted rows
SELECT COUNT(*) AS rows_inserted
FROM RAW_OPEN_BREWERY_DB_DATA
WHERE RAW_RESPONSE IS NOT NULL;