from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Dict, List, Tuple

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from .db import get_engine, session_scope
from .logging_setup import configure_logging
from .config import get_settings
import structlog
from datetime import datetime, timezone

from .models import Base, ClosedPosition, Market, User, ActivePosition
from .polymarket_client import LeaderboardEntry, PolymarketClient


async def ensure_schema() -> None:
    async_engine = get_engine()
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def normalize_closed_position(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map raw Polymarket JSON of a closed position to DB fields.
    Placeholder until the real API shape is known.
    """
    def _parse_dt(val: Any) -> Any:
        if isinstance(val, datetime):
            return val
        if isinstance(val, str):
            try:
                # Handle 'YYYY-MM-DDTHH:MM:SSZ' → UTC
                if val.endswith("Z"):
                    return datetime.fromisoformat(val.replace("Z", "+00:00"))
                # Handle date-only 'YYYY-MM-DD'
                if len(val) == 10 and val[4] == "-" and val[7] == "-":
                    return datetime.strptime(val, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                # Fallback: fromisoformat (with possible timezone)
                return datetime.fromisoformat(val)
            except Exception:
                return None
        return None

    return {
        # data-api /closed-positions: use conditionId to identify market
        "market_external_id": raw.get("conditionId") or raw.get("marketId") or raw.get("market_id"),
        "market_slug": raw.get("marketSlug") or raw.get("slug") or raw.get("eventSlug"),
        "market_title": raw.get("marketTitle") or raw.get("title"),
        # side in sports isn't Yes/No; keep empty/short to satisfy schema length
        "side": raw.get("side") or "",
        "quantity": raw.get("quantity") or raw.get("totalBought"),
        "entry_avg_price": raw.get("entryAvg") or raw.get("avgPrice"),
        # closed payload lacks exitAvg; curPrice is ~1 for resolved winners
        "exit_avg_price": raw.get("exitAvg") or raw.get("curPrice"),
        "realized_pnl": raw.get("realizedPnl"),
        "fees_total": raw.get("fees"),
        "opened_at": _parse_dt(raw.get("openedAt")),
        # sometimes only endDate is present; parse to datetime
        "closed_at": _parse_dt(raw.get("closedAt")) or _parse_dt(raw.get("endDate")),
        "close_reason": raw.get("closeReason"),
        # ensure uniqueness using on-chain asset id when no tx hash is provided
        "tx_hash": raw.get("txHash") or raw.get("asset"),
        "raw_json": json.dumps(raw, ensure_ascii=False),
    }


def normalize_active_position(raw: Dict[str, Any]) -> Dict[str, Any]:
    # Parse end_date if present (YYYY-MM-DD)
    end_dt = None
    if isinstance(raw.get("endDate"), str):
        try:
            end_dt = datetime.strptime(raw["endDate"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            end_dt = None

    return {
        "asset": raw.get("asset"),
        "condition_id": raw.get("conditionId"),
        "size": raw.get("size"),
        "avg_price": raw.get("avgPrice"),
        "initial_value": raw.get("initialValue"),
        "current_value": raw.get("currentValue"),
        "cash_pnl": raw.get("cashPnl"),
        "percent_pnl": raw.get("percentPnl"),
        "total_bought": raw.get("totalBought"),
        "realized_pnl": raw.get("realizedPnl"),
        "current_price": raw.get("curPrice"),
        "redeemable": raw.get("redeemable"),
        "mergeable": raw.get("mergeable"),
        "title": raw.get("title"),
        "slug": raw.get("slug"),
        "icon": raw.get("icon"),
        "event_id": raw.get("eventId"),
        "event_slug": raw.get("eventSlug"),
        "outcome": raw.get("outcome"),
        "outcome_index": raw.get("outcomeIndex"),
        "end_date": end_dt,
        "negative_risk": raw.get("negativeRisk"),
        "raw_json": json.dumps(raw, ensure_ascii=False),
    }


async def upsert_user(session, entry: LeaderboardEntry) -> User:
    existing = (await session.execute(select(User).where(User.user_id == entry.user_id))).scalar_one_or_none()
    if existing:
        if entry.display_name and existing.display_name != entry.display_name:
            existing.display_name = entry.display_name
        return existing
    obj = User(user_id=entry.user_id, display_name=entry.display_name)
    session.add(obj)
    await session.flush()
    return obj


async def bulk_upsert_markets(session, norms: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Ensure all markets from normalized closed positions exist.
    Returns mapping market_external_id -> market_pk.
    """
    market_ids = {str(n.get("market_external_id")) for n in norms if n.get("market_external_id")}
    if not market_ids:
        return {}

    # Load existing
    existing_rows = (
        await session.execute(select(Market).where(Market.market_id.in_(list(market_ids))))
    ).scalars().all()
    id_map: Dict[str, int] = {m.market_id: m.id for m in existing_rows}

    # Prepare missing inserts
    missing_ids = [mid for mid in market_ids if mid not in id_map]
    if missing_ids:
        rows_to_insert: List[Dict[str, Any]] = []
        slug_title_map: Dict[str, Tuple[str | None, str | None]] = {}
        for n in norms:
            mid = str(n.get("market_external_id"))
            if not mid or mid in slug_title_map:
                continue
            slug_title_map[mid] = (n.get("market_slug"), n.get("market_title"))
        for mid in missing_ids:
            slug, title = slug_title_map.get(mid, (None, None))
            rows_to_insert.append({"market_id": mid, "slug": slug, "title": title})

        if rows_to_insert:
            stmt = (
                pg_insert(Market)
                .values(rows_to_insert)
                .on_conflict_do_nothing(index_elements=[Market.__table__.c.market_id])
            )
            await session.execute(stmt)
            # Reload to capture IDs
            existing_rows = (
                await session.execute(select(Market).where(Market.market_id.in_(list(market_ids))))
            ).scalars().all()
            id_map = {m.market_id: m.id for m in existing_rows}

    return id_map


async def bulk_insert_closed_positions(session, user: User, norms: List[Dict[str, Any]], market_id_map: Dict[str, int]) -> int:
    """Insert closed positions in bulk; ignore duplicates by unique constraint."""
    if not norms:
        return 0
    rows: List[Dict[str, Any]] = []
    for n in norms:
        mid = str(n.get("market_external_id")) if n.get("market_external_id") is not None else None
        market_pk = market_id_map.get(mid) if mid is not None else None
        if not market_pk:
            continue
        rows.append({
            "user_pk": user.id,
            "market_pk": market_pk,
            "side": n.get("side") or "",
            "quantity": n.get("quantity"),
            "entry_avg_price": n.get("entry_avg_price"),
            "exit_avg_price": n.get("exit_avg_price"),
            "realized_pnl": n.get("realized_pnl"),
            "fees_total": n.get("fees_total"),
            "opened_at": n.get("opened_at"),
            "closed_at": n.get("closed_at"),
            "close_reason": n.get("close_reason"),
            "tx_hash": n.get("tx_hash"),
            "raw_json": n.get("raw_json"),
        })
    if not rows:
        return 0
    settings = get_settings()
    total_inserted = 0
    for i in range(0, len(rows), settings.insert_batch_size):
        chunk = rows[i:i + settings.insert_batch_size]
        stmt = pg_insert(ClosedPosition).values(chunk).on_conflict_do_nothing(constraint="uq_positions_closed_dedupe")
        await session.execute(stmt)
        total_inserted += len(chunk)
    return total_inserted


async def bulk_upsert_active_positions(session, user: User, norms: List[Dict[str, Any]]) -> int:
    if not norms:
        return 0
    now_dt = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    for n in norms:
        # Skip invalid/incomplete rows to avoid NOT NULL violations
        if not n.get("asset") or n.get("size") is None or n.get("avg_price") is None:
            continue
        payload: Dict[str, Any] = {k: v for k, v in n.items() if k in ActivePosition.__table__.columns}
        payload["user_pk"] = user.id
        payload["updated_at"] = now_dt
        rows.append(payload)
    if not rows:
        return 0
    # Deduplicate within the batch by unique key (user_pk, asset) to avoid
    # "ON CONFLICT DO UPDATE command cannot affect row a second time"
    unique_by_key: Dict[tuple[int, str], Dict[str, Any]] = {}
    for r in rows:
        key = (r["user_pk"], str(r["asset"]))
        unique_by_key[key] = r  # keep the last occurrence
    rows = list(unique_by_key.values())
    settings = get_settings()
    total_upserted = 0
    for i in range(0, len(rows), settings.insert_batch_size):
        chunk = rows[i:i + settings.insert_batch_size]
        insert_stmt = pg_insert(ActivePosition)
        # Build update mapping tied to this insert statement's EXCLUDED
        updatable_cols = [
            c.name for c in ActivePosition.__table__.columns
            if c.name not in {"id", "user_pk", "asset"}
        ]
        update_dict = {col: getattr(insert_stmt.excluded, col) for col in updatable_cols}
        stmt = insert_stmt.values(chunk).on_conflict_do_update(
            constraint="uq_positions_active_user_asset",
            set_=update_dict,
        )
        await session.execute(stmt)
        total_upserted += len(chunk)
    return total_upserted


async def ingest_once(limit: int = 500, active_max_total: int | None = None, closed_max_total: int | None = None) -> None:
    configure_logging()
    log = structlog.get_logger()
    await ensure_schema()

    # Quick test mode via env
    quick = os.getenv("QUICK_TEST", "0").lower() in {"1", "true", "yes"}
    if quick:
        limit = min(limit, 2)
        active_max_total = 10 if active_max_total is None else min(active_max_total, 10)
        closed_max_total = 10 if closed_max_total is None else min(closed_max_total, 10)

    async with PolymarketClient() as client:
        leaderboard = await client.fetch_leaderboard_top(limit=limit, time_period="month", order_by="PNL", category="overall")
        log.info("leaderboard_fetched", count=len(leaderboard))

        sem = asyncio.Semaphore(get_settings().max_concurrency)

        async def process_entry(idx: int, entry: LeaderboardEntry) -> None:
            async with sem:
                log.info("user_start", idx=idx, user=entry.user_id, name=entry.display_name)
                closed_raw_coro = client.fetch_user_closed_positions(entry.user_id, max_total=closed_max_total)
                active_raw_coro = client.fetch_user_active_positions(entry.user_id, max_total=active_max_total)
                closed_raw, active_raw = await asyncio.gather(closed_raw_coro, active_raw_coro)
                async with session_scope() as session:
                    user = await upsert_user(session, entry)

                    # Normalize
                    closed_norms = [normalize_closed_position(r) for r in closed_raw]
                    active_norms = []
                    for r in active_raw:
                        an = normalize_active_position(r)
                        an["icon"] = None  # drop large payloads
                        active_norms.append(an)

                    try:
                        # Markets and closed positions in bulk
                        market_id_map = await bulk_upsert_markets(session, closed_norms)
                        closed_saved = await bulk_insert_closed_positions(session, user, closed_norms, market_id_map)

                        # Active positions in bulk upsert
                        active_saved = await bulk_upsert_active_positions(session, user, active_norms)

                        log.info("user_done", user=entry.user_id, closed_saved=closed_saved, active_saved=active_saved)
                    except Exception as e:
                        # Compact error logging, avoid giant parameter dumps
                        err_msg = str(e)
                        if len(err_msg) > 800:
                            err_msg = err_msg[:800] + "..."
                        log.error("user_failed", user=entry.user_id, error_type=type(e).__name__, error=err_msg)

        await asyncio.gather(*(process_entry(idx, entry) for idx, entry in enumerate(leaderboard, start=1)))


if __name__ == "__main__":
    asyncio.run(ingest_once())



