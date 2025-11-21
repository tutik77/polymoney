from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv


load_dotenv(override=True)


@dataclass(frozen=True)
class Settings:
    database_url: str
    polymarket_base_url: str
    request_timeout_seconds: float
    max_concurrency: int
    requests_per_second: float
    # HTTP pagination tuning
    leaderboard_limit: int
    leaderboard_page_size: int
    closed_positions_page_size: int
    active_positions_page_size: int
    # DB/ingest batching
    insert_batch_size: int
    # DB pool tuning
    db_pool_size: int
    db_max_overflow: int
    # Trading (CLOB)
    polymarket_clob_host: str
    chain_id: int
    private_key: str
    proxy_wallet: Optional[str]
    # Adaptive rate limiting
    min_requests_per_second: float
    max_requests_per_second: float
    adapt_up_successes: int
    adapt_down_factor: float
    adapt_up_factor: float
    # Activities ingest
    activities_page_size: int
    activities_poll_interval_seconds: float
    activities_ts_grace_seconds: float
    # Activities pagination cap per poll
    activities_max_pages_per_poll: int
    # Celery
    celery_broker_url: str
    celery_result_backend: str
    # Elasticsearch
    elasticsearch_hosts: str
    elasticsearch_enabled: bool
    # Automatic settlement
    settlement_interval_minutes: int
    force_settle_after_days: int


def get_settings() -> Settings:
    return Settings(
        database_url=os.getenv(
            "DATABASE_URL",
            "postgresql+asyncpg://polymoney:polymoney@localhost:5432/polymoney",
        ),
        polymarket_base_url=os.getenv("POLYMARKET_BASE_URL", "https://polymarket.com"),
        request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", "20")),
        max_concurrency=int(os.getenv("MAX_CONCURRENCY", "8")),
        requests_per_second=float(os.getenv("REQUESTS_PER_SECOND", "6")),
        leaderboard_limit=int(os.getenv("LEADERBOARD_LIMIT", "500")),
        leaderboard_page_size=int(os.getenv("LEADERBOARD_PAGE_SIZE", "200")),
        closed_positions_page_size=int(os.getenv("CLOSED_POSITIONS_PAGE_SIZE", "100")),
        active_positions_page_size=int(os.getenv("ACTIVE_POSITIONS_PAGE_SIZE", "100")),
        insert_batch_size=int(os.getenv("INSERT_BATCH_SIZE", "500")),
        db_pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
        db_max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
        polymarket_clob_host=os.getenv(
            "POLYMARKET_CLOB_HOST", "https://clob.polymarket.com"
        ),
        chain_id=int(os.getenv("POLY_CHAIN_ID", "137")),
        private_key=os.getenv("POLY_PRIVATE_KEY", ""),
        proxy_wallet=os.getenv("POLY_PROXY_WALLET", None),
        min_requests_per_second=float(os.getenv("MIN_REQUESTS_PER_SECOND", "1")),
        max_requests_per_second=float(
            os.getenv("MAX_REQUESTS_PER_SECOND", os.getenv("REQUESTS_PER_SECOND", "6"))
        ),
        adapt_up_successes=int(os.getenv("ADAPT_UP_SUCCESSES", "12")),
        adapt_down_factor=float(os.getenv("ADAPT_DOWN_FACTOR", "0.8")),
        adapt_up_factor=float(os.getenv("ADAPT_UP_FACTOR", "1.2")),
        activities_page_size=int(os.getenv("ACTIVITIES_PAGE_SIZE", "100")),
        activities_poll_interval_seconds=float(
            os.getenv("ACTIVITIES_POLL_INTERVAL_SECONDS", "60")
        ),
        activities_ts_grace_seconds=float(
            os.getenv("ACTIVITIES_TS_GRACE_SECONDS", "30")
        ),
        activities_max_pages_per_poll=int(
            os.getenv("ACTIVITIES_MAX_PAGES_PER_POLL", "10")
        ),
        celery_broker_url=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
        celery_result_backend=os.getenv(
            "CELERY_RESULT_BACKEND", "redis://localhost:6379/0"
        ),
        elasticsearch_hosts=os.getenv("ELASTICSEARCH_HOSTS", "http://localhost:9200"),
        elasticsearch_enabled=os.getenv("ELASTICSEARCH_ENABLED", "false").lower()
        == "true",
        settlement_interval_minutes=int(
            os.getenv("SETTLEMENT_INTERVAL_MINUTES", "480")
        ),
        force_settle_after_days=int(os.getenv("FORCE_SETTLE_AFTER_DAYS", "3")),
    )
