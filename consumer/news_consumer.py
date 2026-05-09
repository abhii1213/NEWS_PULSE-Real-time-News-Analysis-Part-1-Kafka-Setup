import os
import time
import json
from dotenv import load_dotenv
from kafka import KafkaConsumer
from utils.logger import get_logger
from utils.s3_client import S3Client
from pathlib import Path

#load_dotenv(dotenv_path="../.env")
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

logger = get_logger("news_consumer")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC                  = os.getenv("KAFKA_TOPIC", "raw-news")
CONSUMER_BATCH_SIZE          = int(os.getenv("CONSUMER_BATCH_SIZE", 50))
CONSUMER_FLUSH_INTERVAL_SECS = int(os.getenv("CONSUMER_FLUSH_INTERVAL_SECONDS", 120))
CONSUMER_GROUP_ID            = "news-pipeline-consumer-group"

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME        = os.getenv("S3_BUCKET_NAME")
S3_PREFIX             = os.getenv("S3_PREFIX", "bronze")


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,           # manual commit after successful S3 upload
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=5000,
        max_poll_records=100,
    )


def flush_batch(batch: list[dict], s3: S3Client) -> bool:
    if not batch:
        return True
    logger.info(f"Flushing batch of {len(batch)} articles to S3...")
    uri = s3.upload_batch(batch)
    return uri is not None


def main():
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME]):
        raise ValueError("AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME must be set in .env")

    logger.info("=" * 60)
    logger.info("  News Pipeline Consumer Starting")
    logger.info(f"  Kafka Topic  : {KAFKA_TOPIC}")
    logger.info(f"  S3 Bucket    : {S3_BUCKET_NAME}")
    logger.info(f"  S3 Prefix    : {S3_PREFIX}")
    logger.info(f"  Batch Size   : {CONSUMER_BATCH_SIZE} messages")
    logger.info(f"  Flush Every  : {CONSUMER_FLUSH_INTERVAL_SECS}s")
    logger.info("=" * 60)

    s3 = S3Client(
        bucket=S3_BUCKET_NAME,
        prefix=S3_PREFIX,
        region=AWS_REGION,
        access_key=AWS_ACCESS_KEY_ID,
        secret_key=AWS_SECRET_ACCESS_KEY
    )

    if not s3.verify_connection():
        raise RuntimeError("Cannot reach S3. Fix credentials/bucket before starting consumer.")

    consumer  = build_consumer()
    batch     = []
    last_flush = time.time()
    total_sent = 0

    logger.info(f"✅ Connected to Kafka | Consuming from '{KAFKA_TOPIC}'")

    try:
        while True:
            try:
                for message in consumer:
                    batch.append(message.value)

                    # Flush condition 1 — batch size reached
                    if len(batch) >= CONSUMER_BATCH_SIZE:
                        if flush_batch(batch, s3):
                            consumer.commit()
                            total_sent += len(batch)
                            logger.info(f"📦 Batch flushed | Total uploaded: {total_sent}")
                            batch.clear()
                            last_flush = time.time()
                        else:
                            logger.error("S3 upload failed — will retry on next flush cycle")

            except StopIteration:
                pass  # consumer_timeout_ms hit — no new messages, normal

            # Flush condition 2 — time interval reached
            elapsed = time.time() - last_flush
            if batch and elapsed >= CONSUMER_FLUSH_INTERVAL_SECS:
                logger.info(f"⏱ Time-based flush triggered ({elapsed:.0f}s elapsed)")
                if flush_batch(batch, s3):
                    consumer.commit()
                    total_sent += len(batch)
                    logger.info(f"📦 Batch flushed | Total uploaded: {total_sent}")
                    batch.clear()
                    last_flush = time.time()
            elif not batch:
                logger.info("No new messages — waiting for next poll...")
                last_flush = time.time()

    except KeyboardInterrupt:
        logger.info("Stopped by user (Ctrl+C)")
        if batch:
            logger.info(f"Flushing remaining {len(batch)} articles before exit...")
            if flush_batch(batch, s3):
                consumer.commit()
                logger.info("✅ Final batch uploaded cleanly")
    finally:
        consumer.close()
        logger.info(f"Consumer closed. Total articles uploaded this session: {total_sent}")


if __name__ == "__main__":
    main()