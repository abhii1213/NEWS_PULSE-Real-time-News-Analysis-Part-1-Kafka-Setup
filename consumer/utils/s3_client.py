import os
import json
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime, timezone
from utils.logger import get_logger

logger = get_logger("s3_client")


class S3Client:
    """
    Uploads NDJSON batch files to S3 under a medallion-style prefix:

      s3://{bucket}/bronze/YYYY/MM/DD/HH/batch_<timestamp>.json

    Partitioning by date+hour makes Databricks partition pruning efficient
    when you query specific time ranges later.

    Each file is newline-delimited JSON (NDJSON) — one article per line.
    Spark reads NDJSON natively with spark.read.json().
    """

    def __init__(self, bucket: str, prefix: str, region: str,
                 access_key: str, secret_key: str):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def _make_s3_key(self) -> str:
        """
        Generate a partitioned S3 key:
        bronze/YYYY/MM/DD/HH/batch_YYYYMMDDHHmmss_ffffff.json
        """
        now       = datetime.now(timezone.utc)
        date_part = now.strftime("%Y/%m/%d/%H")
        timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
        return f"{self.prefix}/{date_part}/batch_{timestamp}.json"

    def _to_ndjson(self, articles: list[dict]) -> bytes:
        """One JSON object per line — Spark reads this natively."""
        lines = [
            json.dumps(article, ensure_ascii=False, default=str)
            for article in articles
        ]
        return "\n".join(lines).encode("utf-8")

    def upload_batch(self, articles: list[dict]) -> str | None:
        """
        Upload a batch of articles to S3 as NDJSON.
        Returns the full S3 URI on success, None on failure.
        """
        if not articles:
            logger.warning("upload_batch called with empty list — skipping")
            return None

        s3_key  = self._make_s3_key()
        payload = self._to_ndjson(articles)

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=payload,
                ContentType="application/x-ndjson",
                Metadata={
                    "article-count": str(len(articles)),
                    "pipeline":      "news-pipeline",
                    "layer":         "bronze"
                }
            )
            s3_uri = f"s3://{self.bucket}/{s3_key}"
            logger.info(f"✅ Uploaded {len(articles)} articles ({len(payload)} bytes) → {s3_uri}")
            return s3_uri

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error(f"❌ S3 ClientError [{error_code}]: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error uploading to S3: {e}")
            return None

    def verify_connection(self) -> bool:
        """
        Verify AWS credentials and bucket access before starting the consumer.
        Does a lightweight HeadBucket call — no data transferred.
        """
        try:
            self.client.head_bucket(Bucket=self.bucket)
            logger.info(f"✅ S3 connection verified — bucket '{self.bucket}' is accessible")
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logger.error(f"❌ Bucket '{self.bucket}' does not exist — check S3_BUCKET_NAME in .env")
            elif error_code in ("403", "401"):
                logger.error(f"❌ Access denied to bucket '{self.bucket}' — check IAM permissions")
            else:
                logger.error(f"❌ S3 error [{error_code}]: {e}")
            return False
        except NoCredentialsError:
            logger.error("❌ AWS credentials not found — check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env")
            return False