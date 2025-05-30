-- Ensure correct schema context
USE {{params.database_name}}.GOLD;

-- Create or replace the aggregate view in the gold layer
CREATE OR REPLACE VIEW GOLD.AGG_OPEN_BREWERY_DB_BREWERIES_BY_TYPE_AND_LOCATION AS
SELECT 
    BREWERY_TYPE,
    COUNTRY,
    STATE_PROVINCE,
    COUNT(DISTINCT ID) AS NUM_BREWERIES  -- Ensure ID uniqueness before using DISTINCT
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