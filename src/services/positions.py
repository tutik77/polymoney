from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..models import ActivePosition, ClosedPosition, Market, User


async def bulk_upsert_markets(session: AsyncSession, norms: List[Dict[str, Any]]) -> Dict[str, int]:
    market_ids = {str(n.get("market_external_id")) for n in norms if n.get("market_external_id")}
    if not market_ids:
        return {}

    existing_rows = (
        await session.execute(select(Market).where(Market.market_id.in_(list(market_ids))))
    ).scalars().all()
    id_map: Dict[str, int] = {m.market_id: m.id for m in existing_rows}

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
            existing_rows = (
                await session.execute(select(Market).where(Market.market_id.in_(list(market_ids))))
            ).scalars().all()
            id_map = {m.market_id: m.id for m in existing_rows}

    return id_map


async def bulk_insert_closed_positions(session: AsyncSession, user: User, norms: List[Dict[str, Any]], market_id_map: Dict[str, int]) -> int:
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
            "asset": n.get("asset"),
            "quantity": n.get("quantity"),
            "entry_avg_price": n.get("entry_avg_price"),
            "exit_avg_price": n.get("exit_avg_price"),
            "realized_pnl": n.get("realized_pnl"),
            "closed_at": n.get("closed_at"),
            "title": n.get("title") or n.get("market_title"),
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


async def bulk_upsert_active_positions(session: AsyncSession, user: User, norms: List[Dict[str, Any]]) -> int:
    if not norms:
        return 0
    from datetime import datetime, timezone
    now_dt = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    for n in norms:
        if not n.get("asset") or n.get("size") is None or n.get("avg_price") is None:
            continue
        payload: Dict[str, Any] = {k: v for k, v in n.items() if k in ActivePosition.__table__.columns}
        payload["user_pk"] = user.id
        payload["updated_at"] = now_dt
        rows.append(payload)
    if not rows:
        return 0
    unique_by_key: Dict[tuple[int, str], Dict[str, Any]] = {}
    for r in rows:
        key = (r["user_pk"], str(r["asset"]))
        unique_by_key[key] = r
    rows = list(unique_by_key.values())
    settings = get_settings()
    total_upserted = 0
    for i in range(0, len(rows), settings.insert_batch_size):
        chunk = rows[i:i + settings.insert_batch_size]
        insert_stmt = pg_insert(ActivePosition)
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


async def get_closed_positions_for_user(session: AsyncSession, user_pk: int) -> List[ClosedPosition]:
    from sqlalchemy import select
    return (
        await session.execute(
            select(ClosedPosition).where(ClosedPosition.user_pk == user_pk).order_by(ClosedPosition.closed_at.desc())
        )
    ).scalars().all()


async def get_active_positions_for_user(session: AsyncSession, user_pk: int) -> List[ActivePosition]:
    from sqlalchemy import select
    return (
        await session.execute(
            select(ActivePosition).where(ActivePosition.user_pk == user_pk).order_by(ActivePosition.updated_at.desc())
        )
    ).scalars().all()



