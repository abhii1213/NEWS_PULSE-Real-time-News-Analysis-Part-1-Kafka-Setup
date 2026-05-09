import requests
from datetime import datetime, timedelta, timezone
from utils.logger import get_logger

logger = get_logger("news_client")

# NewsAPI free tier supports these categories
CATEGORIES = ["business", "technology", "science", "health", "sports", "entertainment"]

class NewsAPIClient:
    BASE_URL = "https://newsapi.org/v2/top-headlines"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"X-Api-Key": self.api_key})

    def fetch_articles(self, category: str = "technology", page_size: int = 20) -> list[dict]:
        """
        Fetch top headlines for a given category.
        Returns a list of enriched article dicts.
        """
        params = {
            "category": category,
            "language": "en",
            "pageSize": page_size,
            "country": "us"
        }

        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "ok":
                logger.error(f"NewsAPI returned non-ok status: {data}")
                return []

            articles = data.get("articles", [])
            logger.info(f"Fetched {len(articles)} articles for category='{category}'")

            # Enrich each article with pipeline metadata
            enriched = []
            for article in articles:
                # Skip articles with missing critical fields
                if not article.get("title") or not article.get("url"):
                    continue
                if article.get("title") == "[Removed]":
                    continue

                article["_category"]         = category
                article["_ingested_at"]      = datetime.now(timezone.utc).isoformat()
                article["_source_name"]      = (article.get("source") or {}).get("name", "unknown")
                article["_pipeline_version"] = "v1"

                enriched.append(article)

            return enriched

        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching category='{category}'")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for category='{category}': {e}")
            return []

    def fetch_all_categories(self, page_size: int = 10) -> list[dict]:
        """Fetch articles across all supported categories."""
        all_articles = []
        for category in CATEGORIES:
            articles = self.fetch_articles(category=category, page_size=page_size)
            all_articles.extend(articles)
        return all_articles