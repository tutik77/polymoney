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
    asset: str
    leader_address: str
    quantity: float
    avg_cost: float
    payout: float
    realized_pnl: float
    settlement_type: str
    end_date: Optional[datetime] = None
    title: Optional[str] = None


@dataclass
class SettlementResult:
    sim_user: str
    settled_count: int
    total_pnl: float
    total_cash_change: float
    positions: List[SettledPosition]


async def _settle_single_position(
    session,
    pos: SimActivePosition,
    payout: float,
    settlement_type: str,
    now: datetime,
    sim_user: str,
    settled_positions: List[SettledPosition],
) -> None:
    asset = pos.asset
    quantity = float(pos.quantity)
    avg_cost = float(pos.avg_cost)
    leader_address = pos.leader_address or "unknown"
    
    redemption_cash = payout * quantity
    realized_pnl = (payout - avg_cost) * quantity

    await increment_global_portfolio(
        session,
        sim_user=sim_user,
        cash_delta=redemption_cash,
        realized_pnl_delta=realized_pnl,
    )

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

    await delete_sim_active_position(
        session,
        sim_user=sim_user,
        leader_address=leader_address,
        asset=asset,
    )

    settled_positions.append(
        SettledPosition(
            asset=asset,
            leader_address=leader_address,
            quantity=quantity,
            avg_cost=avg_cost,
            payout=payout,
            realized_pnl=realized_pnl,
            settlement_type=settlement_type,
            end_date=pos.end_date,
            title=pos.title,
        )
    )


async def settle_resolved_positions(
    sim_user: str,
    *,
    force_settle_after_days: int = 3,
) -> SettlementResult:
    log = structlog.get_logger()
    now = datetime.now(timezone.utc)
    force_settle_threshold = now - timedelta(days=force_settle_after_days)

    settled_positions: List[SettledPosition] = []
    total_pnl = 0.0
    total_cash_change = 0.0

    async with session_scope() as session:
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

        positions_by_condition: Dict[str, List[SimActivePosition]] = {}
        positions_without_condition: List[SimActivePosition] = []
        
        for pos in active_positions:
            cond_id = pos.condition_id
            if not cond_id:
                positions_without_condition.append(pos)
                continue
            if cond_id not in positions_by_condition:
                positions_by_condition[cond_id] = []
            positions_by_condition[cond_id].append(pos)

        if positions_without_condition:
            log.warning(
                "settlement_no_condition",
                count=len(positions_without_condition),
                sim_user=sim_user,
            )

        async with PolymarketClient() as client:
            for condition_id, cond_positions in positions_by_condition.items():
                market_data = await client.fetch_market_by_condition_id(condition_id)

                if not market_data:
                    log.debug(
                        "settlement_market_not_found",
                        condition_id=condition_id[:32],
                        count=len(cond_positions),
                    )
                    # Если рынок не найден (например, еще не закрыт и мы фильтруем closed=true),
                    # мы НЕ сеттлим позицию сразу. Мы ждем force_settle_after_days (3 дня).
                    # Если через 3 дня рынок все еще не найден/не закрыт - возвращаем средства (avg_cost).
                    for pos in cond_positions:
                        # Проверяем, прошло ли 3 дня с end_date
                        if pos.end_date and pos.end_date < force_settle_threshold:
                            await _settle_single_position(
                                session=session,
                                pos=pos,
                                payout=float(pos.avg_cost),
                                settlement_type="expired_no_market",
                                now=now,
                                sim_user=sim_user,
                                settled_positions=settled_positions,
                            )
                            total_pnl += 0.0
                            total_cash_change += float(pos.avg_cost) * float(pos.quantity)
                    continue

                tokens = market_data.get("tokens", [])
                is_closed = market_data.get("closed", False)
                
                token_prices: Dict[str, float] = {}
                for token in tokens:
                    token_id = token.get("token_id")
                    price = token.get("price")
                    if token_id and price is not None:
                        try:
                            token_prices[str(token_id)] = float(price)
                        except (ValueError, TypeError):
                            pass

                for pos in cond_positions:
                    asset = pos.asset
                    quantity = float(pos.quantity)
                    avg_cost = float(pos.avg_cost)
                    end_date = pos.end_date

                    payout: Optional[float] = None
                    settlement_type: str = ""

                    if asset in token_prices:
                        price = token_prices[asset]
                        if abs(price - 1.0) < 0.01:
                            payout = 1.0
                            settlement_type = "resolved_won"
                        elif abs(price) < 0.01:
                            payout = 0.0
                            settlement_type = "resolved_lost"
                        elif is_closed:
                            payout = price
                            settlement_type = "resolved_partial"
                    
                    if payout is None and end_date and end_date < force_settle_threshold:
                        payout = avg_cost
                        settlement_type = "expired"
                    
                    if payout is None:
                        continue

                    await _settle_single_position(
                        session=session,
                        pos=pos,
                        payout=payout,
                        settlement_type=settlement_type,
                        now=now,
                        sim_user=sim_user,
                        settled_positions=settled_positions,
                    )
                    
                    redemption_cash = payout * quantity
                    realized_pnl = (payout - avg_cost) * quantity
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
