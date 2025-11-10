from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from .config import get_settings
from .models import Base


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.database_url,
            future=True,
            pool_pre_ping=True,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            hide_parameters=True,
        )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    session = get_session_factory()()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()



async def ensure_schema() -> None:
    """Create all tables if they do not exist."""
    async_engine = get_engine()
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def dispose_engine() -> None:
    """Dispose engine and session factory to avoid cross-event-loop reuse in Celery tasks."""
    global _engine, _session_factory
    try:
        if _engine is not None:
            await _engine.dispose()
    finally:
        _engine = None
        _session_factory = None


