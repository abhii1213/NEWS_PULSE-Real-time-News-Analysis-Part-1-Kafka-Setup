--CREATE BRONZE TABLE
USE SCHEMA NEWS_PIPELINE.BRONZE;

CREATE TABLE IF NOT EXISTS RAW_NEWS (
    RAW_DATA        VARIANT,               -- entire article JSON blob
    FILE_NAME       VARCHAR,               -- which S3 file it came from
    LOADED_AT       TIMESTAMP_NTZ          -- when Snowpipe ingested it
        DEFAULT CURRENT_TIMESTAMP()
);