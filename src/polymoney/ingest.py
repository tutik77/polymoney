from __future__ import annotations

import asyncio

from .logging_setup import configure_logging
from .services.ingest import ingest_top_leaderboard_once


async def ensure_schema() -> None:  # deprecated shim
    from .db import ensure_schema as _ensure
    await _ensure()


async def ingest_once(limit: int = 500, active_max_total: int | None = None, closed_max_total: int | None = None) -> None:
    configure_logging()
    await ensure_schema()
    await ingest_top_leaderboard_once(limit=limit, active_max_total=active_max_total, closed_max_total=closed_max_total)


if __name__ == "__main__":
    asyncio.run(ingest_once())



