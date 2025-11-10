from __future__ import annotations

from typing import List, Optional, Dict
import asyncio

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import User
from ..dependencies import get_db_session
from ..schemas import ActivityOut, ActivePositionOut, ClosedPositionOut, FollowRequest, UserOut
from ...services.users import get_user_by_address, list_users
from ...services.positions import get_active_positions_for_user, get_closed_positions_for_user
from ...services.activities import get_activities_for_user, follow_user_loop


router = APIRouter(prefix="/users", tags=["users"])

# Track running activity following tasks
_activity_tasks: Dict[str, asyncio.Task] = {}
@router.get("/", response_model=List[UserOut])
async def get_users(limit: int = 100, offset: int = 0, session: AsyncSession = Depends(get_db_session)) -> List[UserOut]:
    rows = await list_users(session, limit=limit, offset=offset)
    return [
        UserOut.model_validate({
            "id": r.id,
            "user_id": r.user_id,
            "display_name": r.display_name,
        })
        for r in rows
    ]



async def _get_user(session: AsyncSession, address: str) -> User:
    user = await get_user_by_address(session, address)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.get("/{address}", response_model=UserOut)
async def get_user(address: str, session: AsyncSession = Depends(get_db_session)) -> UserOut:
    user = await _get_user(session, address)
    return UserOut.model_validate({"id": user.id, "user_id": user.user_id, "display_name": user.display_name})


@router.get("/{address}/closed-positions", response_model=List[ClosedPositionOut])
async def get_closed_positions(address: str, session: AsyncSession = Depends(get_db_session)) -> List[ClosedPositionOut]:
    user = await _get_user(session, address)
    rows = await get_closed_positions_for_user(session, user.id)
    return [
        ClosedPositionOut.model_validate({
            "id": r.id,
            "user_pk": r.user_pk,
            "market_pk": r.market_pk,
            "side": r.side,
            "quantity": float(r.quantity) if r.quantity is not None else None,
            "entry_avg_price": float(r.entry_avg_price) if r.entry_avg_price is not None else None,
            "exit_avg_price": float(r.exit_avg_price) if r.exit_avg_price is not None else None,
            "realized_pnl": float(r.realized_pnl) if r.realized_pnl is not None else None,
            "fees_total": float(r.fees_total) if r.fees_total is not None else None,
            "opened_at": r.opened_at,
            "closed_at": r.closed_at,
            "close_reason": r.close_reason,
            "tx_hash": r.tx_hash,
        })
        for r in rows
    ]


@router.get("/{address}/active-positions", response_model=List[ActivePositionOut])
async def get_active_positions(address: str, session: AsyncSession = Depends(get_db_session)) -> List[ActivePositionOut]:
    user = await _get_user(session, address)
    rows = await get_active_positions_for_user(session, user.id)
    return [
        ActivePositionOut.model_validate({
            "id": r.id,
            "user_pk": r.user_pk,
            "asset": r.asset,
            "condition_id": r.condition_id,
            "size": float(r.size),
            "avg_price": float(r.avg_price),
            "initial_value": float(r.initial_value) if r.initial_value is not None else None,
            "current_value": float(r.current_value) if r.current_value is not None else None,
            "cash_pnl": float(r.cash_pnl) if r.cash_pnl is not None else None,
            "percent_pnl": float(r.percent_pnl) if r.percent_pnl is not None else None,
            "total_bought": float(r.total_bought) if r.total_bought is not None else None,
            "realized_pnl": float(r.realized_pnl) if r.realized_pnl is not None else None,
            "current_price": float(r.current_price) if r.current_price is not None else None,
            "redeemable": r.redeemable,
            "mergeable": r.mergeable,
            "title": r.title,
            "slug": r.slug,
            "icon": r.icon,
            "event_id": r.event_id,
            "event_slug": r.event_slug,
            "outcome": r.outcome,
            "outcome_index": r.outcome_index,
            "end_date": r.end_date,
            "negative_risk": r.negative_risk,
            "updated_at": r.updated_at,
        })
        for r in rows
    ]


@router.get("/{address}/activities", response_model=List[ActivityOut])
async def get_activities(address: str, session: AsyncSession = Depends(get_db_session)) -> List[ActivityOut]:
    user = await _get_user(session, address)
    rows = await get_activities_for_user(session, user.id, limit=1000)
    return [
        ActivityOut.model_validate({
            "id": r.id,
            "user_pk": r.user_pk,
            "ts": r.ts,
            "type": r.type,
            "side": r.side,
            "asset": r.asset,
            "condition_id": r.condition_id,
            "price": float(r.price) if r.price is not None else None,
            "size": float(r.size) if r.size is not None else None,
            "fee": float(r.fee) if r.fee is not None else None,
            "tx_hash": r.tx_hash,
        })
        for r in rows
    ]


@router.post("/{address}/activities/follow")
async def start_following_activities(address: str, payload: FollowRequest) -> dict:
    """Start following user's activities and saving to DB."""
    if address in _activity_tasks and not _activity_tasks[address].done():
        return {"status": "already_running"}
    
    task = asyncio.create_task(
        follow_user_loop(
            user_address=address,
            display_name=payload.display_name,
            poll_interval=payload.poll_interval or 60.0,
            bootstrap=payload.bootstrap or False,
        )
    )
    _activity_tasks[address] = task
    return {"status": "started", "address": address}


@router.delete("/{address}/activities/follow")
async def stop_following_activities(address: str) -> dict:
    """Stop following user's activities."""
    task = _activity_tasks.get(address)
    if task and not task.done():
        task.cancel()
        return {"status": "stopped", "address": address}
    return {"status": "not_running", "address": address}


@router.get("/activities/following")
async def list_following() -> dict:
    """List all users being followed for activities."""
    statuses = {addr: (not t.done()) for addr, t in _activity_tasks.items()}
    return {"following": statuses}



