-- 1. Use the DATA_ENGINEER role and AWS credentials to create the Snowflake stage in your bronze schema.
USE ROLE DATA_ENGINEER;
USE SCHEMA ARCADE_DB.BRONZE;

CREATE OR REPLACE STAGE open_brewery_db_stg
    URL = 's3://BUCKET_NAME/airflow_stage/open_brewery_db/raw_api_responses'
CREDENTIALS = (
  AWS_KEY_ID = 'AWS_KEY_ID'
  AWS_SECRET_KEY = 'AWS_SECRET_KEY'
)
FILE_FORMAT = (TYPE = 'JSON');

-- 2. Test the stage
LIST @open_brewery_db_stg;

-- 3. Create a table to receive raw data in the bronze layer.
CREATE TABLE IF NOT EXISTS RAW_OPEN_BREWERY_DB_DATA (
    RAW_RESPONSE VARIANT
);

-- 4. Create a table in the silver layer for processed data.
USE SCHEMA ARCADE_DB.SILVER;

CREATE TABLE IF NOT EXISTS PROCESSED_OPEN_BREWERY_DB_DATA (
    ID STRING COMMENT 'Unique identifier for the brewery',
    NAME STRING COMMENT 'Name of the brewery',
    BREWERY_TYPE STRING COMMENT 'Type of brewery (e.g., micro, nano, brewpub)',
    ADDRESS_1 STRING COMMENT 'Primary street address',
    ADDRESS_2 STRING COMMENT 'Secondary address (nullable)',
    ADDRESS_3 STRING COMMENT 'Tertiary address (nullable)',
    CITY STRING COMMENT 'City where the brewery is located',
    STATE_PROVINCE STRING COMMENT 'State or province of the brewery',
    POSTAL_CODE STRING COMMENT 'Postal or ZIP code of the brewery',
    COUNTRY STRING COMMENT 'Country where the brewery is located',
    LONGITUDE STRING COMMENT 'Longitude coordinate of the brewery',
    LATITUDE STRING COMMENT 'Latitude coordinate of the brewery',
    PHONE STRING COMMENT 'Contact phone number of the brewery',
    WEBSITE_URL STRING COMMENT 'Website URL of the brewery',
    STATE STRING COMMENT 'State where the brewery is located',
    STREET STRING COMMENT 'Street name of the brewery address'
)
COMMENT = 'Table storing brewery information including addresses, contact details, and geographical coordinates';

-- 5. Create an aggregated view in the gold layer for brewery counts per type and location.
USE SCHEMA ARCADE_DB.GOLD;

CREATE OR REPLACE VIEW AGG_OPEN_BREWERY_DB_BREWERIES_BY_TYPE_AND_LOCATION AS
SELECT 
    BREWERY_TYPE,
    COUNTRY,
    STATE_PROVINCE,
    COUNT(DISTINCT ID) AS NUM_BREWERIES
FROM 
    SILVER.PROCESSED_OPEN_BREWERY_DB_DATA
GROUP BY 
    BREWERY_TYPE,
    COUNTRY,
    STATE_PROVINCE
ORDER BY
    LOWER(BREWERY_TYPE),
    LOWER(COUNTRY),
    LOWER(STATE_PROVINCE);
