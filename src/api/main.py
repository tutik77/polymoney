from __future__ import annotations

from fastapi import FastAPI
import asyncio
import structlog

from ..db import ensure_schema
from ..logging_setup import configure_logging
from .routers import admin, health, sim, trading, users, logs


def create_app() -> FastAPI:
    app = FastAPI(title="Polymoney API", version="0.1.0")

    app.include_router(health.router)
    app.include_router(users.router)
    app.include_router(admin.router)
    app.include_router(trading.router)
    app.include_router(sim.router)
    app.include_router(logs.router)

    @app.on_event("startup")
    async def _startup() -> None:  # pragma: no cover - side-effectful
        configure_logging()
        log = structlog.get_logger()
        # Retry DB readiness to avoid race with Postgres in Docker
        last_exc: Exception | None = None
        for attempt in range(1, 21):
            try:
                await ensure_schema()
                last_exc = None
                break
            except Exception as e:  # noqa: BLE001
                last_exc = e
                log.warning("db_retry", attempt=attempt, error=str(e)[:100])
                await asyncio.sleep(min(1 * attempt, 5))
        if last_exc is not None:
            # surface error if never connected
            raise last_exc

    return app


app = create_app()


if __name__ == "__main__":  # pragma: no cover - manual run
    import uvicorn

    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=False)
