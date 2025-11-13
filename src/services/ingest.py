from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import structlog

from ..config import get_settings
from ..db import session_scope
from ..normalizers import normalize_active_position, normalize_closed_position
from ..polymarket_client import LeaderboardEntry, PolymarketClient
from .positions import bulk_insert_closed_positions, bulk_upsert_active_positions, bulk_upsert_markets
from .users import get_or_create_user, recompute_all_users_stats


async def ingest_top_leaderboard_once(limit: int | None = None, active_max_total: int | None = None, closed_max_total: int | None = None) -> None:
    log = structlog.get_logger()
    settings = get_settings()
    limit = settings.leaderboard_limit if limit is None else limit
    closed_max_total = 10000 if closed_max_total is None else closed_max_total
    active_max_total = active_max_total

    async with PolymarketClient() as client:
        leaderboard = await client.fetch_leaderboard_top(limit=limit, time_period="month", order_by="PNL", category="overall")
        log.info("ingest_start", users=len(leaderboard))

        sem = asyncio.Semaphore(settings.max_concurrency)

        async def process_entry(idx: int, entry: LeaderboardEntry) -> None:
            async with sem:
                closed_raw_coro = client.fetch_user_closed_positions(
                    entry.user_id,
                    page_size=settings.closed_positions_page_size,
                    max_total=closed_max_total,
                    sort_by="timestamp",
                    sort_direction="DESC",
                )
                active_raw_coro = client.fetch_user_active_positions(
                    entry.user_id,
                    page_size=settings.active_positions_page_size,
                    max_total=active_max_total,
                )
                closed_raw, active_raw = await asyncio.gather(closed_raw_coro, active_raw_coro)
                async with session_scope() as session:
                    user = await get_or_create_user(session, entry.user_id, entry.display_name)

                    closed_norms = [normalize_closed_position(r) for r in closed_raw]
                    active_norms: List[Dict[str, Any]] = []
                    for r in active_raw:
                        an = normalize_active_position(r)
                        an["icon"] = None
                        active_norms.append(an)

                    try:
                        market_id_map = await bulk_upsert_markets(session, closed_norms)
                        closed_saved = await bulk_insert_closed_positions(session, user, closed_norms, market_id_map)
                        active_saved = await bulk_upsert_active_positions(session, user, active_norms)
                        log.info("ingest_user", user=entry.user_id, closed=closed_saved, active=active_saved)
                    except Exception as e:
                        log.error("ingest_user_error", user=entry.user_id, error=str(e)[:200])

        await asyncio.gather(*(process_entry(idx, entry) for idx, entry in enumerate(leaderboard, start=1)))

        # Recompute aggregated user stats after ingest completes
        async with session_scope() as session:
            await recompute_all_users_stats(session)



