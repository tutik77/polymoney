from __future__ import annotations

from datetime import datetime
from typing import Optional, List

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import case, cast, Float, update

from ..models import Activity, User, ClosedPosition


async def get_user_by_address(session: AsyncSession, address: str) -> Optional[User]:
    return (
        await session.execute(select(User).where(User.user_id == address))
    ).scalar_one_or_none()


async def get_or_create_user(
    session: AsyncSession, address: str, display_name: Optional[str]
) -> User:
    existing = await get_user_by_address(session, address)
    if existing:
        if display_name and existing.display_name != display_name:
            existing.display_name = display_name
        return existing
    obj = User(user_id=address, display_name=display_name)
    session.add(obj)
    await session.flush()
    return obj


async def get_last_seen_activity_ts(
    session: AsyncSession, user_pk: int
) -> Optional[datetime]:
    return (
        await session.execute(
            select(func.max(Activity.ts)).where(Activity.user_pk == user_pk)
        )
    ).scalar_one_or_none()


async def list_users(
    session: AsyncSession, limit: int = 100, offset: int = 0
) -> List[User]:
    return (
        (
            await session.execute(
                select(User).order_by(User.id).limit(limit).offset(offset)
            )
        )
        .scalars()
        .all()
    )


async def recompute_stats_for_user(session: AsyncSession, user_pk: int) -> None:
    """Recompute closed_positions_count and win_rate for a single user."""
    agg = (
        await session.execute(
            select(
                func.count(ClosedPosition.id),
                (
                    cast(
                        func.sum(
                            case(
                                (ClosedPosition.realized_pnl > 0, 1),
                                else_=0,
                            )
                        ),
                        Float,
                    )
                    / func.nullif(func.count(ClosedPosition.id), 0)
                ),
            ).where(ClosedPosition.user_pk == user_pk)
        )
    ).one_or_none()
    if not agg:
        return
    closed_count, win_rate = agg
    await session.execute(
        update(User)
        .where(User.id == user_pk)
        .values(
            closed_positions_count=int(closed_count or 0),
            win_rate=float(win_rate) if win_rate is not None else None,
        )
    )


async def recompute_all_users_stats(session: AsyncSession) -> None:
    """Recompute stats for all users that have at least one closed position."""
    rows = (
        (
            await session.execute(
                select(
                    ClosedPosition.user_pk,
                    func.count(ClosedPosition.id).label("closed_count"),
                    (
                        cast(
                            func.sum(
                                case(
                                    (ClosedPosition.realized_pnl > 0, 1),
                                    else_=0,
                                )
                            ),
                            Float,
                        )
                        / func.nullif(func.count(ClosedPosition.id), 0)
                    ).label("win_rate"),
                ).group_by(ClosedPosition.user_pk)
            )
        )
        .mappings()
        .all()
    )
    for r in rows:
        await session.execute(
            update(User)
            .where(User.id == r["user_pk"])
            .values(
                closed_positions_count=int(r["closed_count"] or 0),
                win_rate=float(r["win_rate"]) if r["win_rate"] is not None else None,
            )
        )
