from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import structlog

from ..db import ensure_schema, session_scope
from ..models import ClosedPosition, User
from ..polymarket_client import LeaderboardEntry, PolymarketClient
from .ingest import ingest_top_leaderboard_once
from .positions import get_closed_positions_for_user
from .users import get_user_by_address


@dataclass(frozen=True)
class UserSimStats:
    user_id: str
    display_name: Optional[str]
    positions_count: int
    gross_pnl: float
    fees: float
    net_pnl: float


@dataclass(frozen=True)
class DryRunSummary:
    top_n: int
    initial_cash: float
    total_gross_pnl: float
    total_fees: float
    total_net_pnl: float
    final_cash: float
    per_user: List[UserSimStats]


def _as_float(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)  # Decimal -> float if needed
    except Exception:
        return None


def _pnl_from_closed_position(p: ClosedPosition) -> Tuple[float, float, float]:
    qty = _as_float(p.quantity) or 0.0
    entry = _as_float(p.entry_avg_price) or 0.0
    exit_ = _as_float(p.exit_avg_price) or 0.0
    fees = _as_float(p.fees_total) or 0.0
    gross = qty * (exit_ - entry)
    net = gross - fees
    return gross, fees, net


async def _fetch_top_leaderboard(limit: int) -> List[LeaderboardEntry]:
    async with PolymarketClient() as client:
        # Default: month/PNL/overall — matches current ingest
        return await client.fetch_leaderboard_top(
            limit=limit, time_period="month", order_by="PNL", category="overall"
        )


async def _ensure_users_present(entries: List[LeaderboardEntry]) -> None:
    # Ensure corresponding User rows exist after ingest; ingest will create them, but
    # in case ingest is skipped, we at least create minimal rows.
    from .users import get_or_create_user

    async with session_scope() as session:
        for e in entries:
            existing = await get_user_by_address(session, e.user_id)
            if not existing:
                await get_or_create_user(session, e.user_id, e.display_name)


async def run_closed_positions_dry_run(
    top_n: int = 10, initial_cash: float = 10_000_000.0, *, refresh_ingest: bool = True
) -> DryRunSummary:
    """
    Simple dry-run using closed positions of top-N leaderboard users.

    Assumptions:
    - Replicate 1:1 quantities and prices from closed positions.
    - Net PnL = quantity * (exit_avg_price - entry_avg_price) - fees_total.
    - Cash updated by net PnL only (large bankroll assumption; no capital constraints modeled).
    """
    log = structlog.get_logger()

    # Ensure DB schema exists
    await ensure_schema()

    # Optionally refresh local DB from the network to have fresh closed positions
    if refresh_ingest:
        await ingest_top_leaderboard_once(
            limit=top_n, active_max_total=None, closed_max_total=10_000
        )

    # Pull top-N addresses (from network to avoid relying on stale DB ranking)
    entries = await _fetch_top_leaderboard(limit=top_n)
    await _ensure_users_present(entries)

    per_user_stats: List[UserSimStats] = []
    total_gross = 0.0
    total_fees = 0.0
    total_net = 0.0

    async with session_scope() as session:
        for e in entries:
            user_row: Optional[User] = await get_user_by_address(session, e.user_id)
            if not user_row:
                continue
            closed: List[ClosedPosition] = await get_closed_positions_for_user(
                session, user_row.id
            )
            gross_sum = 0.0
            fees_sum = 0.0
            net_sum = 0.0
            for p in closed:
                g, f, n = _pnl_from_closed_position(p)
                gross_sum += g
                fees_sum += f
                net_sum += n

            per_user_stats.append(
                UserSimStats(
                    user_id=e.user_id,
                    display_name=e.display_name,
                    positions_count=len(closed),
                    gross_pnl=gross_sum,
                    fees=fees_sum,
                    net_pnl=net_sum,
                )
            )
            total_gross += gross_sum
            total_fees += fees_sum
            total_net += net_sum

    final_cash = initial_cash + total_net

    summary = DryRunSummary(
        top_n=top_n,
        initial_cash=initial_cash,
        total_gross_pnl=total_gross,
        total_fees=total_fees,
        total_net_pnl=total_net,
        final_cash=final_cash,
        per_user=per_user_stats,
    )
    log.info("sim_dry_done", users=top_n, pnl=total_net, cash=final_cash)
    return summary
