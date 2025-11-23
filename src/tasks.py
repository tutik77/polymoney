from __future__ import annotations

import asyncio
from typing import Optional
import structlog

from billiard.exceptions import SoftTimeLimitExceeded
from .celery_app import celery_app
from .services.ingest import ingest_top_leaderboard_once
from .services.activities import follow_user_loop
from .services.sim_realtime import follow_user_realtime_sim
from .services.settlement import settle_resolved_positions
from .db import ensure_schema, dispose_engine
from sqlalchemy import select


log = structlog.get_logger()


@celery_app.task(name="polymoney.ingest_leaderboard", bind=True)
def ingest_leaderboard_task(
    self,
    limit: Optional[int] = None,
    active_max_total: Optional[int] = None,
    closed_max_total: Optional[int] = None,
):
    async def _run():
        await dispose_engine()  # ensure engine is bound to this loop
        await ensure_schema()
        await ingest_top_leaderboard_once(
            limit=limit,
            active_max_total=active_max_total,
            closed_max_total=closed_max_total,
        )
        await dispose_engine()  # clean up after finishing

    asyncio.run(_run())
    log.info("task_done", task="ingest", id=self.request.id)
    return {"status": "completed", "task_id": self.request.id}


@celery_app.task(name="polymoney.follow_user_activities", bind=True)
def follow_user_activities_task(
    self,
    user_address: str,
    display_name: Optional[str] = None,
    poll_interval: Optional[float] = None,
    bootstrap: bool = False,
):
    async def _run():
        await dispose_engine()
        await ensure_schema()
        try:
            await follow_user_loop(
                user_address=user_address,
                display_name=display_name,
                poll_interval=poll_interval,
                bootstrap=bootstrap,
            )
        except asyncio.CancelledError:
            log.info(
                "task_cancelled", task="follow", user=user_address, id=self.request.id
            )
            raise
        finally:
            await dispose_engine()

    try:
        asyncio.run(_run())
    except (asyncio.CancelledError, SoftTimeLimitExceeded):
        return {"status": "cancelled", "user": user_address, "task_id": self.request.id}

    log.info("task_done", task="follow", user=user_address, id=self.request.id)
    return {"status": "completed", "user": user_address, "task_id": self.request.id}


@celery_app.task(name="polymoney.sim_realtime", bind=True)
def sim_realtime_task(
    self,
    user_address: str,
    display_name: Optional[str] = None,
    sim_user_id: str = "default",
    initial_cash: float = 10_000_000.0,
    poll_interval: Optional[float] = None,
    slippage_bps: float = 0.0,
    sizing_strategy: str = "target_profit",
    sizing_value: float = 0.005,
):
    async def _run():
        await dispose_engine()
        await ensure_schema()
        try:
            await follow_user_realtime_sim(
                user_address=user_address,
                display_name=display_name,
                sim_user_id=sim_user_id,
                initial_cash=initial_cash,
                poll_interval=poll_interval,
                slippage_bps=slippage_bps,
                sizing_strategy=sizing_strategy,
                sizing_value=sizing_value,
            )
        except asyncio.CancelledError:
            log.info(
                "task_cancelled", task="sim", user=user_address, id=self.request.id
            )
            raise
        finally:
            await dispose_engine()

    try:
        asyncio.run(_run())
    except (asyncio.CancelledError, SoftTimeLimitExceeded):
        # Normalize Celery soft timeouts and explicit revokes as "cancelled"
        return {"status": "cancelled", "user": user_address, "task_id": self.request.id}

    log.info("task_done", task="sim", user=user_address, id=self.request.id)
    return {"status": "completed", "user": user_address, "task_id": self.request.id}


@celery_app.task(name="polymoney.settle_positions", bind=True)
def settle_positions_task(
    self,
    sim_user: str,
    force_settle_after_days: int = 3,
    fetch_limit: int = 1000,
):
    async def _run():
        await dispose_engine()
        await ensure_schema()
        result = await settle_resolved_positions(
            sim_user=sim_user,
            force_settle_after_days=force_settle_after_days,
            fetch_limit=fetch_limit,
        )
        return {
            "settled_count": result.settled_count,
            "total_pnl": result.total_pnl,
            "total_cash_change": result.total_cash_change,
        }

    result = asyncio.run(_run())
    # Best-effort cleanup outside loop isn't needed here since _run disposes engine
    log.info(
        "task_done", task="settle", sim_user=sim_user, id=self.request.id, **result
    )
    return {
        "status": "completed",
        "sim_user": sim_user,
        "task_id": self.request.id,
        **result,
    }


@celery_app.task(name="polymoney.settle_positions_all", bind=True)
def settle_positions_all_task(
    self,
    force_settle_after_days: int = 3,
    fetch_limit: int = 1000,
):
    async def _run():
        await dispose_engine()
        try:
            await ensure_schema()
            # Discover simulators from global portfolios
            from .db import session_scope
            from .models import SimPortfolioGlobal

            sim_users: list[str] = []
            async with session_scope() as session:
                rows = await session.execute(
                    select(SimPortfolioGlobal.sim_user).distinct()
                )
                sim_users = [r[0] for r in rows.all() if r and r[0]]
            totals = {
                "settled_count": 0,
                "total_pnl": 0.0,
                "total_cash_change": 0.0,
                "sim_users": sim_users,
            }
            for su in sim_users or ["default"]:
                try:
                    res = await settle_resolved_positions(
                        sim_user=su,
                        force_settle_after_days=force_settle_after_days,
                        fetch_limit=fetch_limit,
                    )
                    totals["settled_count"] += res.settled_count
                    totals["total_pnl"] += res.total_pnl
                    totals["total_cash_change"] += res.total_cash_change
                except Exception as e:
                    log.error("settle_user_error", sim_user=su, error=str(e)[:200])
            return totals
        finally:
            await dispose_engine()

    result = asyncio.run(_run())
    log.info("task_done", task="settle_all", id=self.request.id, **result)
    return {"status": "completed", "task_id": self.request.id, **result}
