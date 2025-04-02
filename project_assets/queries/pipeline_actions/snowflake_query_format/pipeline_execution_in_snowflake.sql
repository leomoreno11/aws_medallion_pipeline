-- Ensure correct schema context and clear old data
USE SCHEMA ARCADE_DB.BRONZE;
DELETE FROM RAW_OPEN_BREWERY_DB_DATA;

-- Load data into the Bronze layer with validation
COPY INTO RAW_OPEN_BREWERY_DB_DATA
FROM @open_brewery_db_stg/open_brewery_db_response.json
FILE_FORMAT = (TYPE = 'JSON')
VALIDATION_MODE = RETURN_ERRORS;

-- Ensure correct schema context for Silver layer processing
USE SCHEMA ARCADE_DB.SILVER;

-- Insert processed data into the Silver layer (ACID-compliant)
INSERT OVERWRITE INTO PROCESSED_OPEN_BREWERY_DB_DATA 
SELECT
    TRY_CAST(f.value:id AS STRING) AS id,
    TRY_CAST(f.value:name AS STRING) AS name,
    TRY_CAST(f.value:brewery_type AS STRING) AS brewery_type,
    TRY_CAST(f.value:address_1 AS STRING) AS address_1,
    TRY_CAST(f.value:address_2 AS STRING) AS address_2,
    TRY_CAST(f.value:address_3 AS STRING) AS address_3,
    TRY_CAST(f.value:city AS STRING) AS city,
    TRY_CAST(f.value:state_province AS STRING) AS state_province,
    TRY_CAST(f.value:postal_code AS STRING) AS postal_code,
    TRY_CAST(f.value:country AS STRING) AS country,
    TRY_CAST(f.value:longitude AS STRING) AS longitude,
    TRY_CAST(f.value:latitude AS STRING) AS latitude,
    TRY_CAST(f.value:phone AS STRING) AS phone,
    TRY_CAST(f.value:website_url AS STRING) AS website_url,
    TRY_CAST(f.value:state AS STRING) AS state,
    TRY_CAST(f.value:street AS STRING) AS street
FROM 
    BRONZE.RAW_OPEN_BREWERY_DB_DATA
CROSS JOIN LATERAL FLATTEN(input => RAW_RESPONSE) f
WHERE RAW_RESPONSE IS NOT NULL;