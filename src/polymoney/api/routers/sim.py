from __future__ import annotations

from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Depends
from celery.result import AsyncResult

from ...tasks import sim_realtime_task, settle_positions_task
from ...celery_app import celery_app
from ...services.sim_realtime import get_all_sim_snapshots, get_sim_snapshot
from ...services.sim_db import (
    get_sim_portfolio as db_get_sim_portfolio,
    get_sim_active_positions as db_get_sim_active_positions,
    get_sim_closed_positions as db_get_sim_closed_positions,
    get_sim_trades as db_get_sim_trades,
)
from ..schemas import (
    SimRealtimeStartRequest,
    SimPortfolioOut,
    SimPositionOut,
    SimTradeOut,
    SimActivePositionDbOut,
    SimClosedPositionDbOut,
    SettlementResultOut,
    SettledPositionOut,
)
from ..dependencies import get_db_session
from sqlalchemy.ext.asyncio import AsyncSession


router = APIRouter(prefix="/sim", tags=["sim"])


@router.post("/realtime/start")
async def sim_realtime_start(payload: SimRealtimeStartRequest) -> dict:
    users = payload.users or []
    started: Dict[str, dict] = {}

    # Build a set of running/queued realtime sim keys to avoid duplicates:
    # key format: f"{sim_user}:{address_lower}"
    sim_user = payload.sim_user or "default"
    inspector = celery_app.control.inspect()
    existing_keys: set[str] = set()
    existing_ids: Dict[str, str] = {}
    def _collect(task_list):
        for t in task_list or []:
            if t.get("name") == "polymoney.sim_realtime":
                kwargs = t.get("kwargs", {})
                u = (kwargs.get("user_address") or "").lower()
                s = kwargs.get("sim_user_id", "default")
                if u:
                    key = f"{s}:{u}"
                    existing_keys.add(key)
                    if t.get("id"):
                        existing_ids[key] = t["id"]
    try:
        active = inspector.active() or {}
        for _, tasks in active.items():
            _collect(tasks)
    except Exception:
        pass
    try:
        reserved = inspector.reserved() or {}
        for _, tasks in reserved.items():
            _collect(tasks)
    except Exception:
        pass
    try:
        scheduled = inspector.scheduled() or {}
        for _, entries in scheduled.items():
            # scheduled returns entries with 'request'
            for e in entries or []:
                req = e.get("request", {})
                _collect([req])
    except Exception:
        pass

    for addr in users:
        addr_l = (addr or "").lower()
        key = f"{sim_user}:{addr_l}"
        if key in existing_keys:
            started[addr] = {"status": "already_running", "task_id": existing_ids.get(key)}
            continue
        result = sim_realtime_task.apply_async(
            kwargs={
                "user_address": addr,
                "display_name": None,
                "sim_user_id": sim_user,
                "initial_cash": payload.initial_cash or 10_000_000.0,
                "poll_interval": payload.poll_interval,
                "slippage_bps": payload.slippage_bps or 0.0,
            }
        )
        started[addr] = {"status": "started", "task_id": result.id}

    return {"tasks": started}


@router.delete("/realtime/stop/{sim_user}/{address}")
async def sim_realtime_stop(sim_user: str, address: str) -> dict:
    inspector = celery_app.control.inspect()
    active_tasks = inspector.active() or {}
    addr_l = (address or "").lower()
    for worker, tasks in active_tasks.items():
        for task in tasks:
            if task.get("name") == "polymoney.sim_realtime":
                task_args = task.get("kwargs", {})
                t_addr = (task_args.get("user_address") or "").lower()
                t_sim = task_args.get("sim_user_id", "default")
                if (t_addr == addr_l and t_sim == sim_user):
                    celery_app.control.revoke(task["id"], terminate=True)
                    return {"status": "cancelled", "task_id": task["id"]}
    
    return {"status": "not_running"}


@router.delete("/realtime/stop/{address}")
async def sim_realtime_stop_default(address: str) -> dict:
    return await sim_realtime_stop("default", address)


@router.get("/realtime")
async def sim_realtime_list() -> dict:
    inspector = celery_app.control.inspect()
    active_tasks = inspector.active() or {}
    
    running = {}
    for worker, tasks in active_tasks.items():
        for task in tasks:
            if task.get("name") == "polymoney.sim_realtime":
                task_args = task.get("kwargs", {})
                sim_user = task_args.get("sim_user_id", "default")
                address = task_args.get("user_address")
                if address:
                    key = f"{sim_user}:{address}"
                    running[key] = {"task_id": task["id"], "worker": worker}
    
    return {"tasks": running}


@router.get("/realtime/portfolio", response_model=dict[str, SimPortfolioOut])
async def sim_realtime_portfolios() -> dict[str, SimPortfolioOut]:
    """Aggregated snapshot for all running realtime simulations."""
    snaps = get_all_sim_snapshots()
    out: dict[str, SimPortfolioOut] = {}
    for addr, s in snaps.items():
        out[addr] = SimPortfolioOut(
            user=s.user,
            cash=s.cash,
            realized_pnl=s.realized_pnl,
            positions=[SimPositionOut(asset=p.asset, quantity=p.quantity, avg_cost=p.avg_cost) for p in s.positions],
            updated_at=s.updated_at,
        )
    return out


@router.get("/realtime/portfolio/{address}", response_model=SimPortfolioOut)
async def sim_realtime_portfolio(address: str) -> SimPortfolioOut:
    s = get_sim_snapshot(address)
    if not s:
        raise HTTPException(status_code=404, detail="simulation not running for this address")
    return SimPortfolioOut(
        user=s.user,
        cash=s.cash,
        realized_pnl=s.realized_pnl,
        positions=[SimPositionOut(asset=p.asset, quantity=p.quantity, avg_cost=p.avg_cost) for p in s.positions],
        updated_at=s.updated_at,
    )


# ---- DB-backed simulated portfolio ----

@router.get("/db/{sim_user}/portfolio")
async def sim_db_portfolio(sim_user: str, session: AsyncSession = Depends(get_db_session)) -> dict:
    row = await db_get_sim_portfolio(session, sim_user)
    if not row:
        raise HTTPException(status_code=404, detail="sim portfolio not found")
    return row


@router.get("/db/{sim_user}/positions/active", response_model=list[SimActivePositionDbOut])
async def sim_db_positions_active(sim_user: str, session: AsyncSession = Depends(get_db_session)) -> list[SimActivePositionDbOut]:
    rows = await db_get_sim_active_positions(session, sim_user)
    return [
        SimActivePositionDbOut(
            sim_user=r.sim_user,
            leader_address=r.leader_address,
            asset=r.asset,
            quantity=float(r.quantity),
            avg_cost=float(r.avg_cost),
            end_date=r.end_date,
            title=r.title,
            condition_id=r.condition_id,
        )
        for r in rows
    ]


@router.get("/db/{sim_user}/positions/closed", response_model=list[SimClosedPositionDbOut])
async def sim_db_positions_closed(sim_user: str, limit: int = 1000, offset: int = 0, session: AsyncSession = Depends(get_db_session)) -> list[SimClosedPositionDbOut]:
    rows = await db_get_sim_closed_positions(session, sim_user, limit=limit, offset=offset)
    return [
        SimClosedPositionDbOut(
            asset=r.asset,
            quantity=float(r.quantity),
            avg_cost=float(r.avg_cost),
            payout=float(r.payout),
            realized_pnl=float(r.realized_pnl),
            closed_at=r.closed_at,
            leader_address=r.leader_address,
        )
        for r in rows
    ]


@router.get("/db/{sim_user}/trades", response_model=list[SimTradeOut])
async def sim_db_trades(sim_user: str, limit: int = 100, offset: int = 0, session: AsyncSession = Depends(get_db_session)) -> list[SimTradeOut]:
    rows = await db_get_sim_trades(session, sim_user, limit=limit, offset=offset)
    return [
        SimTradeOut(
            id=r.id,
            sim_user=r.sim_user,
            leader_address=r.leader_address,
            ts=r.ts,
            side=r.side,
            asset=r.asset,
            price=float(r.price),
            size=float(r.size),
            fee=float(r.fee),
            notional=float(r.notional),
            exec_type=r.exec_type,
            source_tx=r.source_tx,
            source_ts=r.source_ts,
        )
        for r in rows
    ]


@router.post("/{sim_user}/settle")
async def settle_positions(sim_user: str, force_settle_after_days: int = 2) -> dict:
    result = settle_positions_task.apply_async(
        kwargs={
            "sim_user": sim_user,
            "force_settle_after_days": force_settle_after_days,
        }
    )
    return {"status": "scheduled", "task_id": result.id}


