--CREATE SNOWPIPE
USE SCHEMA NEWS_PIPELINE.BRONZE;

CREATE PIPE IF NOT EXISTS NEWS_BRONZE_PIPE
    AUTO_INGEST = TRUE
    COMMENT     = 'Auto-ingest news JSON batches from S3'
AS
COPY INTO RAW_NEWS (RAW_DATA, FILE_NAME, LOADED_AT)
FROM (
    SELECT 
        $1,                                    -- the JSON object (one per line)
        METADATA$FILENAME,                     -- S3 file path
        CURRENT_TIMESTAMP()
    FROM @S3_BRONZE_STAGE
)
FILE_FORMAT = (
    TYPE              = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
);

-- Get the SQS ARN — you need this for S3 event notifications
SHOW PIPES;
SELECT * FROM RAW_NEWS;