from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..models import SimPortfolio, SimPortfolioGlobal, SimActivePosition, SimClosedPosition, SimTrade


async def upsert_sim_portfolio(session: AsyncSession, sim_user: str, leader_address: Optional[str], cash: float, realized_pnl: float) -> None:
    row = (
        await session.execute(
            select(SimPortfolio).where(
                SimPortfolio.sim_user == sim_user, SimPortfolio.leader_address == leader_address
            )
        )
    ).scalar_one_or_none()
    now_dt = datetime.now(timezone.utc)
    if row is None:
        session.add(SimPortfolio(sim_user=sim_user, leader_address=leader_address, cash=cash, realized_pnl=realized_pnl, updated_at=now_dt))
    else:
        row.cash = cash
        row.realized_pnl = realized_pnl
        row.updated_at = now_dt


async def ensure_sim_global(session: AsyncSession, *, sim_user: str, initial_cash: float) -> None:
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
        .on_conflict_do_nothing(index_elements=[SimPortfolioGlobal.__table__.c.sim_user])
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
    Buy into a position, merging with existing if present.
    Returns: (new_quantity, new_avg_cost)
    """
    row = await get_sim_active_position_by_leader_asset(
        session,
        sim_user=sim_user,
        leader_address=leader_address,
        asset=asset,
    )
    
    if row is None:
        # New position
        session.add(
            SimActivePosition(
                sim_user=sim_user,
                leader_address=leader_address,
                asset=asset,
                quantity=size,
                avg_cost=price,
                end_date=end_date,
                title=title,
                condition_id=condition_id,
            )
        )
        return size, price
    else:
        # Merge with existing
        old_qty = float(row.quantity)
        old_cost = float(row.avg_cost)
        
        new_qty = old_qty + size
        # Weighted average cost
        new_avg_cost = ((old_cost * old_qty) + (price * size)) / new_qty if new_qty > 0 else price
        
        row.quantity = new_qty
        row.avg_cost = new_avg_cost
        
        # Update metadata if provided
        if end_date is not None:
            row.end_date = end_date
        if title is not None:
            row.title = title
        if condition_id is not None:
            row.condition_id = condition_id
        
        return new_qty, new_avg_cost


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
    Sell from a position, capped by current holdings.
    Returns: (executed_size, new_quantity, realized_pnl) or None if no position
    """
    row = await get_sim_active_position_by_leader_asset(
        session,
        sim_user=sim_user,
        leader_address=leader_address,
        asset=asset,
    )
    
    if row is None or row.quantity <= 0:
        return None
    
    held = float(row.quantity)
    avg_cost = float(row.avg_cost)
    
    # Cap by holdings
    executed_size = min(size, held)
    new_qty = held - executed_size
    
    # Calculate realized PnL
    realized_pnl = (price - avg_cost) * executed_size
    
    if new_qty <= 0:
        # Position fully closed
        await delete_sim_active_position(
            session,
            sim_user=sim_user,
            leader_address=leader_address,
            asset=asset,
        )
    else:
        # Update remaining quantity
        row.quantity = new_qty
    
    return executed_size, new_qty, realized_pnl


async def delete_sim_active_position(session: AsyncSession, *, sim_user: str, leader_address: Optional[str], asset: str) -> None:
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
        )
    )


# ---- Queries for API ----

async def get_sim_portfolio(session: AsyncSession, sim_user: str) -> Optional[dict]:
    # Prefer global aggregate row if present
    glob = (
        await session.execute(select(SimPortfolioGlobal).where(SimPortfolioGlobal.sim_user == sim_user))
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
        await session.execute(select(SimPortfolio).where(SimPortfolio.sim_user == sim_user))
    ).scalars().all()
    if not rows:
        return None
    cash_sum = sum(float(r.cash) for r in rows)
    realized_sum = sum(float(r.realized_pnl) for r in rows)
    updated_at = max(r.updated_at for r in rows)
    return {"sim_user": sim_user, "cash": cash_sum, "realized_pnl": realized_sum, "updated_at": updated_at}


async def get_sim_active_positions(session: AsyncSession, sim_user: str) -> List[SimActivePosition]:
    rows = (
        await session.execute(
            select(SimActivePosition).where(SimActivePosition.sim_user == sim_user)
        )
    ).scalars().all()
    return list(rows)


async def get_sim_closed_positions(session: AsyncSession, sim_user: str, limit: int = 1000, offset: int = 0) -> List[SimClosedPosition]:
    return (
        await session.execute(
            select(SimClosedPosition)
            .where(SimClosedPosition.sim_user == sim_user)
            .order_by(SimClosedPosition.closed_at.desc())
            .limit(limit)
            .offset(offset)
        )
    ).scalars().all()


async def get_sim_trades(session: AsyncSession, sim_user: str, limit: int = 1000, offset: int = 0) -> List[SimTrade]:
    return (
        await session.execute(
            select(SimTrade)
            .where(SimTrade.sim_user == sim_user)
            .order_by(SimTrade.ts.desc())
            .limit(limit)
            .offset(offset)
        )
    ).scalars().all()


