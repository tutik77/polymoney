"""Settlement service for resolving simulator positions based on leader outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import structlog
from sqlalchemy import select

from ..db import session_scope
from ..models import SimActivePosition
from ..polymarket_client import PolymarketClient
from .sim_db import (
    delete_sim_active_position,
    increment_global_portfolio,
    insert_sim_closed_position,
)


@dataclass
class SettledPosition:
    """Details of a settled position."""

    asset: str
    leader_address: str
    quantity: float
    avg_cost: float
    payout: float
    realized_pnl: float
    settlement_type: str  # "resolved" or "expired"
    end_date: Optional[datetime] = None
    title: Optional[str] = None


@dataclass
class SettlementResult:
    """Result of settlement operation."""

    sim_user: str
    settled_count: int
    total_pnl: float
    total_cash_change: float
    positions: List[SettledPosition]


async def settle_resolved_positions(
    sim_user: str,
    *,
    force_settle_after_days: int = 2,
) -> SettlementResult:
    """
    Settle resolved positions for a simulator.

    Process:
    1. Find SimActivePosition where end_date <= NOW()
    2. Group by leader_address
    3. For each leader, fetch closed positions
    4. Match by asset and settle:
       - If found in leader's closed positions with curPrice = 1.0 or 0.0 → resolve
       - If end_date > force_settle_after_days ago → force settle at avg_cost
    5. Update balances and move to closed positions

    Args:
        sim_user: Simulator user ID
        force_settle_after_days: Days after end_date to force settle (default: 2)

    Returns:
        SettlementResult with details of settled positions
    """
    log = structlog.get_logger()
    now = datetime.now(timezone.utc)
    force_settle_threshold = now - timedelta(days=force_settle_after_days)

    settled_positions: List[SettledPosition] = []
    total_pnl = 0.0
    total_cash_change = 0.0

    async with session_scope() as session:
        # Get active positions where end_date has passed
        stmt = select(SimActivePosition).where(
            SimActivePosition.sim_user == sim_user,
            SimActivePosition.end_date.isnot(None),
            SimActivePosition.end_date <= now,
        )
        result = await session.execute(stmt)
        active_positions = list(result.scalars().all())

        if not active_positions:
            return SettlementResult(
                sim_user=sim_user,
                settled_count=0,
                total_pnl=0.0,
                total_cash_change=0.0,
                positions=[],
            )

        log.info("settlement_start", sim_user=sim_user, positions=len(active_positions))

        # Group by leader
        positions_by_leader: Dict[str, List[SimActivePosition]] = {}
        for pos in active_positions:
            leader = pos.leader_address or "unknown"
            if leader not in positions_by_leader:
                positions_by_leader[leader] = []
            positions_by_leader[leader].append(pos)

        # Process each leader's positions
        async with PolymarketClient() as client:
            for leader_address, positions in positions_by_leader.items():
                if leader_address == "unknown":
                    continue

                # Fetch leader's closed positions (limited to recent 150)
                # Since we run settlement daily, recent positions should be sufficient
                try:
                    closed_positions_raw = await client.fetch_user_closed_positions(
                        leader_address,
                        max_total=500,
                        sort_by="timestamp",
                        sort_direction="DESC",
                    )
                except Exception as e:
                    log.error(
                        "settlement_fetch_error",
                        leader=leader_address,
                        error=str(e)[:200],
                    )
                    continue

                # Build index: asset -> closed position data
                resolved_by_asset: Dict[str, Dict] = {}
                for closed in closed_positions_raw:
                    asset = closed.get("asset")
                    cur_price = closed.get("curPrice")
                    if asset and cur_price is not None:
                        try:
                            price_float = float(cur_price)
                            # Only resolved outcomes (1.0 = won, 0.0 = lost)
                            if abs(price_float - 1.0) < 0.01 or abs(price_float) < 0.01:
                                resolved_by_asset[str(asset)] = {
                                    "payout": 1.0 if price_float >= 0.5 else 0.0,
                                    "raw": closed,
                                }
                        except (ValueError, TypeError):
                            continue

                for pos in positions:
                    asset = pos.asset
                    quantity = float(pos.quantity)
                    avg_cost = float(pos.avg_cost)
                    end_date = pos.end_date

                    payout: Optional[float] = None
                    settlement_type: str = ""

                    if asset in resolved_by_asset:
                        payout = resolved_by_asset[asset]["payout"]
                        settlement_type = "resolved"
                    elif end_date and end_date < force_settle_threshold:
                        payout = avg_cost
                        settlement_type = "expired"
                    else:
                        continue

                    # Calculate settlement
                    redemption_cash = payout * quantity
                    realized_pnl = (payout - avg_cost) * quantity

                    # Update global portfolio
                    await increment_global_portfolio(
                        session,
                        sim_user=sim_user,
                        cash_delta=redemption_cash,
                        realized_pnl_delta=realized_pnl,
                    )

                    # Create closed position record
                    await insert_sim_closed_position(
                        session,
                        sim_user=sim_user,
                        asset=asset,
                        quantity=quantity,
                        avg_cost=avg_cost,
                        payout=payout,
                        realized_pnl=realized_pnl,
                        closed_at=now,
                        leader_address=leader_address,
                        title=pos.title,
                    )

                    # Delete active position
                    await delete_sim_active_position(
                        session,
                        sim_user=sim_user,
                        leader_address=leader_address,
                        asset=asset,
                    )

                    # Track results
                    settled_positions.append(
                        SettledPosition(
                            asset=asset,
                            leader_address=leader_address,
                            quantity=quantity,
                            avg_cost=avg_cost,
                            payout=payout,
                            realized_pnl=realized_pnl,
                            settlement_type=settlement_type,
                            end_date=end_date,
                            title=pos.title,
                        )
                    )
                    total_pnl += realized_pnl
                    total_cash_change += redemption_cash

    result = SettlementResult(
        sim_user=sim_user,
        settled_count=len(settled_positions),
        total_pnl=total_pnl,
        total_cash_change=total_cash_change,
        positions=settled_positions,
    )

    log.info(
        "settlement_done",
        sim_user=sim_user,
        count=result.settled_count,
        pnl=total_pnl,
        cash=total_cash_change,
    )

    return result
