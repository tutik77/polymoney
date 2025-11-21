from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import structlog
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..config import get_settings
from ..db import session_scope
from ..models import Activity, User
from ..normalizers import normalize_activity
from ..polymarket_client import PolymarketClient
from ..utils.datetime import parse_datetime_aware
from .users import get_last_seen_activity_ts, get_or_create_user


async def insert_activities(user: User, items: List[Dict[str, Any]]) -> int:
    if not items:
        return 0
    rows: List[Dict[str, Any]] = []
    seen_sigs: set[tuple] = set()
    for it in items:
        norm = normalize_activity(it)
        if not norm.get("ts"):
            continue
        # Build a dedup signature when tx_hash is missing
        sig = (
            norm.get("tx_hash") or None,
            norm.get("ts"),
            norm.get("type"),
            norm.get("side"),
            norm.get("asset"),
            norm.get("price"),
            norm.get("size"),
        )
        if sig in seen_sigs:
            continue
        seen_sigs.add(sig)
        payload = {
            "user_pk": user.id,
            "ts": norm["ts"],
            "type": norm.get("type"),
            "side": norm.get("side"),
            "asset": norm.get("asset"),
            "condition_id": norm.get("condition_id"),
            "price": norm.get("price"),
            "size": norm.get("size"),
            "fee": norm.get("fee"),
            "title": norm.get("title"),
            "tx_hash": norm.get("tx_hash"),
        }
        rows.append(payload)
    if not rows:
        return 0
    async with session_scope() as session:
        stmt = (
            pg_insert(Activity)
            .values(rows)
            .on_conflict_do_nothing(constraint="uq_activities_user_tx")
        )
        await session.execute(stmt)
    return len(rows)


async def follow_user_loop(
    user_address: str,
    display_name: Optional[str],
    poll_interval: Optional[float] = None,
    bootstrap: bool = False,
) -> None:
    log = structlog.get_logger()
    settings = get_settings()
    interval = (
        poll_interval
        if poll_interval is not None
        else settings.activities_poll_interval_seconds
    )

    async with session_scope() as session:
        user = await get_or_create_user(session, user_address, display_name)
    last_seen_ts = None
    async with session_scope() as session:
        last_seen_ts = await get_last_seen_activity_ts(session, user.id)

    if last_seen_ts is None:
        if bootstrap:
            raw0: List[Dict[str, Any]] = []
            async with PolymarketClient() as client0:
                raw0 = await client0.fetch_user_activities(
                    user_address,
                    page_size=settings.activities_page_size,
                    max_total=settings.activities_page_size,
                )
            raw0_sorted = sorted(
                raw0,
                key=lambda it: parse_datetime_aware(
                    it.get("timestamp")
                    or it.get("time")
                    or it.get("createdAt")
                    or it.get("blockTime")
                )
                or datetime.fromtimestamp(0, tz=timezone.utc),
            )
            if raw0_sorted:
                saved0 = await insert_activities(user, raw0_sorted)
                last_seen_ts = max(
                    (
                        parse_datetime_aware(
                            it.get("timestamp")
                            or it.get("time")
                            or it.get("createdAt")
                            or it.get("blockTime")
                        )
                        for it in raw0_sorted
                    ),
                    default=datetime.now(timezone.utc),
                )
                log.info("follow_bootstrap", user=user_address, count=saved0)
            else:
                last_seen_ts = datetime.now(timezone.utc)
        else:
            last_seen_ts = datetime.now(timezone.utc)

    async with PolymarketClient() as client:
        while True:
            try:
                # Fetch up to N pages to avoid missing bursts
                max_pages = max(1, int(settings.activities_max_pages_per_poll))
                raw = await client.fetch_user_activities(
                    user_address,
                    page_size=settings.activities_page_size,
                    max_total=settings.activities_page_size * max_pages,
                )

                def _key(it: Dict[str, Any]) -> datetime:
                    dt = parse_datetime_aware(
                        it.get("timestamp")
                        or it.get("time")
                        or it.get("createdAt")
                        or it.get("blockTime")
                    ) or datetime.fromtimestamp(0, tz=timezone.utc)
                    return dt

                raw.sort(key=_key)
                new_items: List[Dict[str, Any]] = []
                grace = timedelta(seconds=settings.activities_ts_grace_seconds)
                for it in raw:
                    ts = parse_datetime_aware(
                        it.get("timestamp")
                        or it.get("time")
                        or it.get("createdAt")
                        or it.get("blockTime")
                    )
                    if ts and ts > (last_seen_ts - grace):
                        new_items.append(it)
                if new_items:
                    await insert_activities(user, new_items)
                    log.info("follow_saved", user=user_address, count=len(new_items))
                    max_ts = max(
                        (
                            normalize_activity(it)["ts"]
                            for it in new_items
                            if normalize_activity(it)["ts"]
                        ),
                        default=last_seen_ts,
                    )
                    if max_ts and max_ts > last_seen_ts:
                        last_seen_ts = max_ts
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                log.info("follow_cancelled", user=user_address)
                break
            except Exception as e:
                log.error("follow_error", user=user_address, error=str(e)[:200])
                await asyncio.sleep(min(60.0, interval))


async def get_activities_for_user(
    session, user_pk: int, limit: int = 1000
) -> List[Activity]:
    from sqlalchemy import select

    return (
        (
            await session.execute(
                select(Activity)
                .where(Activity.user_pk == user_pk)
                .order_by(Activity.ts.desc())
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
