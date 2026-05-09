--CREATE S3 EXTERNAL STAGE
USE SCHEMA NEWS_PIPELINE.BRONZE;

CREATE STAGE IF NOT EXISTS S3_BRONZE_STAGE
    URL            = '****************************'
    CREDENTIALS    = (
        AWS_KEY_ID     = '*********************'
        AWS_SECRET_KEY = '****************'
    )
    FILE_FORMAT    = (
        TYPE              = 'JSON'
        STRIP_OUTER_ARRAY = FALSE   -- our files are NDJSON (one JSON per line)
    )
    COMMENT = 'S3 bronze landing zone for raw news articles';

-- Verify Snowflake can see your S3 files
LIST @S3_BRONZE_STAGE;