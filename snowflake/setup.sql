-- ── Warehouse ────────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS NEWS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60        -- suspends after 60s inactivity (saves credits)
    AUTO_RESUME    = TRUE
    COMMENT        = 'News pipeline warehouse';

-- ── Database + Schemas (one per medallion layer) ──────────────
CREATE DATABASE IF NOT EXISTS NEWS_PIPELINE;

CREATE SCHEMA IF NOT EXISTS NEWS_PIPELINE.BRONZE;

-- ── Dedicated role + user for the pipeline ───────────────────
CREATE ROLE IF NOT EXISTS NEWS_PIPELINE_ROLE;

GRANT USAGE  ON WAREHOUSE NEWS_WH          TO ROLE NEWS_PIPELINE_ROLE;
GRANT USAGE  ON DATABASE  NEWS_PIPELINE    TO ROLE NEWS_PIPELINE_ROLE;
GRANT ALL    ON SCHEMA NEWS_PIPELINE.BRONZE TO ROLE NEWS_PIPELINE_ROLE;

-- Grant role to your user
GRANT ROLE NEWS_PIPELINE_ROLE TO USER ABHIIII1213;

USE ROLE      NEWS_PIPELINE_ROLE;
USE WAREHOUSE NEWS_WH;
USE DATABASE  NEWS_PIPELINE;
USE SCHEMA    BRONZE;

SELECT 'Setup complete ✅' AS STATUS;




--add this if you face any issue while deploying the job and scheduling it
USE ROLE ACCOUNTADMIN;

-- Allow dbt to create the PROD schema and all objects inside it
GRANT CREATE SCHEMA ON DATABASE NEWS_PIPELINE TO ROLE NEWS_PIPELINE_ROLE;

-- Also grant on the warehouse
GRANT USAGE ON WAREHOUSE NEWS_WH TO ROLE NEWS_PIPELINE_ROLE;

SELECT 'Production grants done' AS STATUS;




-- ============================================================
-- RUN THIS ONLY WHEN YOU FACE ISSUES WHILE RUNNING YOUR STREAMLIT APP IN PROD
-- BEFORE MAKING STREAMLIT APP IN PROD , MAKE SURE YOU SELECT PROD_GOLD under schema, if it is not coming in dropdown change the overall role to NEWS_PIPELINE_ROLE
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- PROD SCHEMA ACCESS
-- ============================================================

GRANT USAGE ON SCHEMA NEWS_PIPELINE.PROD_STAGING
TO ROLE NEWS_PIPELINE_ROLE;

GRANT USAGE ON SCHEMA NEWS_PIPELINE.PROD_SILVER
TO ROLE NEWS_PIPELINE_ROLE;

GRANT USAGE ON SCHEMA NEWS_PIPELINE.PROD_GOLD
TO ROLE NEWS_PIPELINE_ROLE;

-- ============================================================
-- TABLE ACCESS
-- ============================================================

GRANT SELECT ON ALL TABLES IN SCHEMA NEWS_PIPELINE.PROD_SILVER
TO ROLE NEWS_PIPELINE_ROLE;

GRANT SELECT ON FUTURE TABLES IN SCHEMA NEWS_PIPELINE.PROD_SILVER
TO ROLE NEWS_PIPELINE_ROLE;

GRANT SELECT ON ALL TABLES IN SCHEMA NEWS_PIPELINE.PROD_GOLD
TO ROLE NEWS_PIPELINE_ROLE;

GRANT SELECT ON FUTURE TABLES IN SCHEMA NEWS_PIPELINE.PROD_GOLD
TO ROLE NEWS_PIPELINE_ROLE;

-- ============================================================
-- STREAMLIT CREATION ACCESS
-- ============================================================

GRANT CREATE STREAMLIT ON SCHEMA NEWS_PIPELINE.PROD_GOLD
TO ROLE NEWS_PIPELINE_ROLE;

-- ============================================================
-- Optional but recommended
-- ============================================================

GRANT CREATE STAGE ON SCHEMA NEWS_PIPELINE.PROD_GOLD
TO ROLE NEWS_PIPELINE_ROLE;