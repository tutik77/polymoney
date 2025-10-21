from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_url: str
    polymarket_base_url: str
    request_timeout_seconds: float
    max_concurrency: int
    requests_per_second: float
    # HTTP pagination tuning
    leaderboard_page_size: int
    closed_positions_page_size: int
    active_positions_page_size: int
    # DB/ingest batching
    insert_batch_size: int
    # DB pool tuning
    db_pool_size: int
    db_max_overflow: int


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
        leaderboard_page_size=int(os.getenv("LEADERBOARD_PAGE_SIZE", "200")),
        closed_positions_page_size=int(os.getenv("CLOSED_POSITIONS_PAGE_SIZE", "100")),
        active_positions_page_size=int(os.getenv("ACTIVE_POSITIONS_PAGE_SIZE", "100")),
        insert_batch_size=int(os.getenv("INSERT_BATCH_SIZE", "500")),
        db_pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
        db_max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
    )


