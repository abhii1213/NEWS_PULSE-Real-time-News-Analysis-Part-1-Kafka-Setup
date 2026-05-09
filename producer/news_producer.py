import json
import time
import hashlib
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from utils.logger import get_logger
from utils.news_client import NewsAPIClient
from pathlib import Path

#load_dotenv(dotenv_path="../.env")
# Get project root (2 levels up from this file)
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

logger = get_logger("news_producer")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "raw-news")
NEWS_API_KEY            = os.getenv("NEWS_API_KEY")
POLL_INTERVAL_SECONDS   = int(os.getenv("POLL_INTERVAL_SECONDS", 600))  # fetch every 10 min


def make_message_key(article: dict) -> bytes:
    """
    Deduplicate at the Kafka level using a stable hash of (url).
    Same article published twice → same key → same partition → easy to dedupe later.
    """
    unique_str = article.get("url", "")
    return hashlib.md5(unique_str.encode()).hexdigest().encode("utf-8")


def serialize(data: dict) -> bytes:
    return json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k,           # already bytes
        value_serializer=serialize,
        acks="all",                            # wait for all replicas to ack
        retries=5,
        retry_backoff_ms=500,
        linger_ms=100,                         # batch messages up to 100ms
        compression_type="gzip",              # compress batches
    )


def send_articles(producer: KafkaProducer, articles: list[dict]) -> tuple[int, int]:
    """Send articles to Kafka. Returns (success_count, fail_count)."""
    success, fail = 0, 0

    for article in articles:
        key = make_message_key(article)
        try:
            future = producer.send(KAFKA_TOPIC, key=key, value=article)
            future.add_callback(
                lambda meta: logger.debug(
                    f"Sent → partition={meta.partition} offset={meta.offset}"
                )
            )
            future.add_errback(
                lambda e: logger.error(f"Send failed: {e}")
            )
            success += 1

        except KafkaError as e:
            logger.error(f"KafkaError sending article '{article.get('title', '')}': {e}")
            fail += 1

    producer.flush()  # block until all buffered messages are sent
    return success, fail


def main():
    if not NEWS_API_KEY:
        raise ValueError("NEWS_API_KEY is not set in .env")

    logger.info("=" * 60)
    logger.info("  News Pipeline Producer Starting")
    logger.info(f"  Topic            : {KAFKA_TOPIC}")
    logger.info(f"  Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Poll Interval    : {POLL_INTERVAL_SECONDS}s")
    logger.info("=" * 60)

    # Connect to Kafka with retry
    producer = None
    for attempt in range(1, 6):
        try:
            producer = build_producer()
            logger.info("✅ Connected to Kafka")
            break
        except NoBrokersAvailable:
            logger.warning(f"Kafka not reachable (attempt {attempt}/5) — retrying in 5s...")
            time.sleep(5)

    if not producer:
        raise RuntimeError("Could not connect to Kafka after 5 attempts. Is Docker running?")

    news_client = NewsAPIClient(api_key=NEWS_API_KEY)
    run_count = 0

    try:
        while True:
            run_count += 1
            logger.info(f"── Poll #{run_count} starting at {datetime.now(timezone.utc).isoformat()} ──")

            articles = news_client.fetch_all_categories(page_size=10)

            if articles:
                ok, fail = send_articles(producer, articles)
                logger.info(f"Poll #{run_count} complete → ✅ {ok} sent | ❌ {fail} failed | Total fetched: {len(articles)}")
            else:
                logger.warning("No articles returned from NewsAPI this cycle")

            logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s until next poll...\n")
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user (Ctrl+C)")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed cleanly")


if __name__ == "__main__":
    main()