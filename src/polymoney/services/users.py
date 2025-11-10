from __future__ import annotations

from datetime import datetime
from typing import Optional, List

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Activity, User


async def get_user_by_address(session: AsyncSession, address: str) -> Optional[User]:
    return (
        await session.execute(select(User).where(User.user_id == address))
    ).scalar_one_or_none()


async def get_or_create_user(session: AsyncSession, address: str, display_name: Optional[str]) -> User:
    existing = await get_user_by_address(session, address)
    if existing:
        if display_name and existing.display_name != display_name:
            existing.display_name = display_name
        return existing
    obj = User(user_id=address, display_name=display_name)
    session.add(obj)
    await session.flush()
    return obj


async def get_last_seen_activity_ts(session: AsyncSession, user_pk: int) -> Optional[datetime]:
    return (
        await session.execute(select(func.max(Activity.ts)).where(Activity.user_pk == user_pk))
    ).scalar_one_or_none()



async def list_users(session: AsyncSession, limit: int = 100, offset: int = 0) -> List[User]:
    return (
        await session.execute(
            select(User).order_by(User.id).limit(limit).offset(offset)
        )
    ).scalars().all()


