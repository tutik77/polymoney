from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import select, update, delete, func, case
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..models import (
    SimPortfolio,
    SimPortfolioGlobal,
    SimActivePosition,
    SimClosedPosition,
    SimTrade,
)


async def upsert_sim_portfolio(
    session: AsyncSession,
    sim_user: str,
    leader_address: Optional[str],
    cash: float,
    realized_pnl: float,
) -> None:
    row = (
        await session.execute(
            select(SimPortfolio).where(
                SimPortfolio.sim_user == sim_user,
                SimPortfolio.leader_address == leader_address,
            )
        )
    ).scalar_one_or_none()
    now_dt = datetime.now(timezone.utc)
    if row is None:
        session.add(
            SimPortfolio(
                sim_user=sim_user,
                leader_address=leader_address,
                cash=cash,
                realized_pnl=realized_pnl,
                updated_at=now_dt,
            )
        )
    else:
        row.cash = cash
        row.realized_pnl = realized_pnl
        row.updated_at = now_dt


async def ensure_sim_global(
    session: AsyncSession, *, sim_user: str, initial_cash: float
) -> None:
    now_dt = datetime.now(timezone.utc)
    # Idempotent insert with ON CONFLICT DO NOTHING to avoid race conditions
    stmt = (
        pg_insert(SimPortfolioGlobal)
        .values(
            sim_user=sim_user,
            cash=initial_cash,
            realized_pnl=0.0,
            updated_at=now_dt,
        )
        .on_conflict_do_nothing(
            index_elements=[SimPortfolioGlobal.__table__.c.sim_user]
        )
    )
    await session.execute(stmt)


async def increment_global_portfolio(
    session: AsyncSession,
    *,
    sim_user: str,
    cash_delta: float = 0.0,
    realized_pnl_delta: float = 0.0,
) -> None:
    now_dt = datetime.now(timezone.utc)
    await session.execute(
        update(SimPortfolioGlobal)
        .where(SimPortfolioGlobal.sim_user == sim_user)
        .values(
            cash=SimPortfolioGlobal.cash + cash_delta,
            realized_pnl=SimPortfolioGlobal.realized_pnl + realized_pnl_delta,
            updated_at=now_dt,
        )
    )


async def record_sim_trade(
    session: AsyncSession,
    *,
    sim_user: str,
    leader_address: Optional[str],
    ts: datetime,
    side: str,
    asset: str,
    price: float,
    size: float,
    fee: float,
    notional: float,
    exec_type: Optional[str],
    title: Optional[str],
    source_tx: Optional[str],
    source_ts: Optional[datetime],
) -> None:
    # Deduplicate by source_tx if present
    if source_tx:
        existing = (
            await session.execute(
                select(SimTrade)
                .where(
                    SimTrade.sim_user == sim_user,
                    SimTrade.leader_address == leader_address,
                    SimTrade.source_tx == source_tx,
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if existing:
            return
    session.add(
        SimTrade(
            sim_user=sim_user,
            leader_address=leader_address,
            ts=ts,
            side=side,
            asset=asset,
            price=price,
            size=size,
            fee=fee,
            notional=notional,
            exec_type=exec_type,
            title=title,
            source_tx=source_tx,
            source_ts=source_ts,
        )
    )


async def get_sim_active_position_by_leader_asset(
    session: AsyncSession,
    *,
    sim_user: str,
    leader_address: str,
    asset: str,
) -> Optional[SimActivePosition]:
    """Get active position for specific leader and asset."""
    result = await session.execute(
        select(SimActivePosition).where(
            SimActivePosition.sim_user == sim_user,
            SimActivePosition.leader_address == leader_address,
            SimActivePosition.asset == asset,
        )
    )
    return result.scalar_one_or_none()


async def buy_sim_position(
    session: AsyncSession,
    *,
    sim_user: str,
    leader_address: str,
    asset: str,
    price: float,
    size: float,
    end_date: Optional[datetime] = None,
    title: Optional[str] = None,
    condition_id: Optional[str] = None,
) -> tuple[float, float]:
    """
    Buy into a position atomically (UPSERT).
    Returns: (new_quantity, new_avg_cost)
    """
    ins = pg_insert(SimActivePosition).values(
        sim_user=sim_user,
        leader_address=leader_address,
        asset=asset,
        quantity=size,
        avg_cost=price,
        end_date=end_date,
        title=title,
        condition_id=condition_id,
    )
    stmt = ins.on_conflict_do_update(
        index_elements=[
            SimActivePosition.__table__.c.sim_user,
            SimActivePosition.__table__.c.leader_address,
            SimActivePosition.__table__.c.asset,
        ],
        set_={
            # q_new = q_old + size
            "quantity": SimActivePosition.quantity + ins.excluded.quantity,
            # avg_new = (avg_old*q_old + price*size) / (q_old + size)
            "avg_cost": (
                (SimActivePosition.avg_cost * SimActivePosition.quantity)
                + (ins.excluded.avg_cost * ins.excluded.quantity)
            )
            / (SimActivePosition.quantity + ins.excluded.quantity),
            # Update metadata only if provided in this call
            "end_date": func.coalesce(
                ins.excluded.end_date, SimActivePosition.end_date
            ),
            "title": func.coalesce(ins.excluded.title, SimActivePosition.title),
            "condition_id": func.coalesce(
                ins.excluded.condition_id, SimActivePosition.condition_id
            ),
        },
    ).returning(SimActivePosition.quantity, SimActivePosition.avg_cost)

    res = await session.execute(stmt)
    q, avg = res.first()
    return float(q), float(avg)


async def sell_sim_position(
    session: AsyncSession,
    *,
    sim_user: str,
    leader_address: str,
    asset: str,
    price: float,
    size: float,
) -> Optional[tuple[float, float, float]]:
    """
    Sell from a position with row-level locking to avoid races.
    Returns: (executed_size, new_quantity, realized_pnl) or None if no position
    """
    # Lock the row to serialize concurrent updates
    result = await session.execute(
        select(SimActivePosition)
        .where(
            SimActivePosition.sim_user == sim_user,
            SimActivePosition.leader_address == leader_address,
            SimActivePosition.asset == asset,
        )
        .with_for_update()
    )
    row = result.scalar_one_or_none()
    if row is None or row.quantity <= 0:
        return None

    held = float(row.quantity)
    avg_cost = float(row.avg_cost)

    executed_size = min(size, held)
    if executed_size <= 0:
        return 0.0, held, 0.0

    new_qty = held - executed_size
    realized_pnl = (price - avg_cost) * executed_size

    if new_qty <= 0:
        # Fully close position
        await delete_sim_active_position(
            session,
            sim_user=sim_user,
            leader_address=leader_address,
            asset=asset,
        )
    else:
        row.quantity = new_qty

    return executed_size, new_qty, realized_pnl


async def delete_sim_active_position(
    session: AsyncSession, *, sim_user: str, leader_address: Optional[str], asset: str
) -> None:
    await session.execute(
        delete(SimActivePosition).where(
            SimActivePosition.sim_user == sim_user,
            SimActivePosition.leader_address == leader_address,
            SimActivePosition.asset == asset,
        )
    )


async def insert_sim_closed_position(
    session: AsyncSession,
    *,
    sim_user: str,
    asset: str,
    quantity: float,
    avg_cost: float,
    payout: float,
    realized_pnl: float,
    closed_at: datetime,
    leader_address: Optional[str],
    title: Optional[str],
) -> None:
    session.add(
        SimClosedPosition(
            sim_user=sim_user,
            asset=asset,
            quantity=quantity,
            avg_cost=avg_cost,
            payout=payout,
            realized_pnl=realized_pnl,
            closed_at=closed_at,
            leader_address=leader_address,
            title=title,
        )
    )


# ---- Queries for API ----


async def get_sim_portfolio(session: AsyncSession, sim_user: str) -> Optional[dict]:
    # Prefer global aggregate row if present
    glob = (
        await session.execute(
            select(SimPortfolioGlobal).where(SimPortfolioGlobal.sim_user == sim_user)
        )
    ).scalar_one_or_none()
    if glob:
        return {
            "sim_user": sim_user,
            "cash": float(glob.cash),
            "realized_pnl": float(glob.realized_pnl),
            "updated_at": glob.updated_at,
        }
    # Fallback to legacy per-leader aggregation
    rows = (
        (
            await session.execute(
                select(SimPortfolio).where(SimPortfolio.sim_user == sim_user)
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        return None
    cash_sum = sum(float(r.cash) for r in rows)
    realized_sum = sum(float(r.realized_pnl) for r in rows)
    updated_at = max(r.updated_at for r in rows)
    return {
        "sim_user": sim_user,
        "cash": cash_sum,
        "realized_pnl": realized_sum,
        "updated_at": updated_at,
    }


async def get_sim_active_positions(
    session: AsyncSession, sim_user: str
) -> List[SimActivePosition]:
    rows = (
        (
            await session.execute(
                select(SimActivePosition).where(SimActivePosition.sim_user == sim_user)
            )
        )
        .scalars()
        .all()
    )
    return list(rows)


async def get_sim_closed_positions(
    session: AsyncSession, sim_user: str, limit: int = 1000, offset: int = 0
) -> List[SimClosedPosition]:
    return (
        (
            await session.execute(
                select(SimClosedPosition)
                .where(SimClosedPosition.sim_user == sim_user)
                .order_by(SimClosedPosition.closed_at.desc())
                .limit(limit)
                .offset(offset)
            )
        )
        .scalars()
        .all()
    )


async def get_sim_trades(
    session: AsyncSession, sim_user: str, limit: int = 1000, offset: int = 0
) -> List[SimTrade]:
    return (
        (
            await session.execute(
                select(SimTrade)
                .where(SimTrade.sim_user == sim_user)
                .order_by(SimTrade.ts.desc())
                .limit(limit)
                .offset(offset)
            )
        )
        .scalars()
        .all()
    )


async def get_sim_leader_stats(session: AsyncSession, sim_user: str) -> list[dict]:
    """
    Aggregate per-leader stats for a sim_user:
      - active_count: number of active positions for the leader
      - closed_count: number of closed positions for the leader
      - realized_pnl: sum of realized_pnl from closed positions for the leader
      - win_rate: wins / closed_count, where win = realized_pnl > 0
    Leaders with NULL address are excluded.
    """
    # Active positions count per leader
    active_result = await session.execute(
        select(
            SimActivePosition.leader_address,
            func.count().label("active_count"),
        )
        .where(
            SimActivePosition.sim_user == sim_user,
            SimActivePosition.leader_address.isnot(None),
        )
        .group_by(SimActivePosition.leader_address)
    )
    active_counts = {r.leader_address: int(r.active_count) for r in active_result.all()}

    # Closed positions count and realized PnL per leader
    closed_result = await session.execute(
        select(
            SimClosedPosition.leader_address,
            func.count().label("closed_count"),
            func.coalesce(func.sum(SimClosedPosition.realized_pnl), 0).label(
                "realized_pnl"
            ),
            func.sum(
                case(
                    (SimClosedPosition.realized_pnl > 0, 1),
                    else_=0,
                )
            ).label("wins_count"),
        )
        .where(
            SimClosedPosition.sim_user == sim_user,
            SimClosedPosition.leader_address.isnot(None),
        )
        .group_by(SimClosedPosition.leader_address)
    )
    closed_map = {
        r.leader_address: {
            "closed_count": int(r.closed_count),
            "realized_pnl": float(r.realized_pnl or 0.0),
            "wins_count": int(r.wins_count or 0),
        }
        for r in closed_result.all()
    }

    # Union of leaders from active and closed
    leaders: set[str] = set(active_counts.keys()) | set(closed_map.keys())

    stats: list[dict] = []
    for leader in sorted(leaders):
        a = active_counts.get(leader, 0)
        c = closed_map.get(leader, {}).get("closed_count", 0)
        pnl = closed_map.get(leader, {}).get("realized_pnl", 0.0)
        wins = closed_map.get(leader, {}).get("wins_count", 0)
        win_rate = (wins / c) if c > 0 else 0.0
        stats.append(
            {
                "leader_address": leader,
                "active_count": a,
                "closed_count": c,
                "realized_pnl": pnl,
                "win_rate": win_rate,
            }
        )
    return stats


async def insert_trade_if_new(
    session: AsyncSession,
    *,
    sim_user: str,
    leader_address: Optional[str],
    ts: datetime,
    side: str,
    asset: str,
    price: float,
    size: float,
    fee: float,
    notional: float,
    exec_type: Optional[str],
    title: Optional[str],
    source_tx: Optional[str],
    source_ts: Optional[datetime],
) -> bool:
    """
    Insert trade once using ON CONFLICT DO NOTHING on uq_sim_trades_source_tx.
    Returns True if inserted (new), False if duplicate (already processed).
    If source_tx is NULL, inserts without constraint (caller should gate duplicates).
    """
    stmt = pg_insert(SimTrade.__table__).values(
        sim_user=sim_user,
        leader_address=leader_address,
        ts=ts,
        side=side,
        asset=asset,
        price=price,
        size=size,
        fee=fee,
        notional=notional,
        exec_type=exec_type,
        title=title,
        source_tx=source_tx,
        source_ts=source_ts,
    )
    if source_tx:
        stmt = stmt.on_conflict_do_nothing(constraint="uq_sim_trades_source_tx")
    res = await session.execute(stmt)
    return bool(getattr(res, "rowcount", 0))
