-- Ensure correct schema context
USE {{params.database_name}}.SILVER;

-- Insert data into the silver layer with atomic overwrite
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
    TRY_CAST(f.value:street AS STRING) AS street
FROM 
    BRONZE.RAW_OPEN_BREWERY_DB_DATA
CROSS JOIN LATERAL FLATTEN(input => RAW_RESPONSE) f
WHERE RAW_RESPONSE IS NOT NULL;  -- Avoid unnecessary processing

-- Validate inserted rows to ensure data integrity
SELECT COUNT(*) AS rows_inserted
FROM PROCESSED_OPEN_BREWERY_DB_DATA
WHERE ID IS NOT NULL;