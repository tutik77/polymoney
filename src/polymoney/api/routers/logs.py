from __future__ import annotations

import os
from typing import Literal
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse


router = APIRouter(prefix="/logs", tags=["logs"])

AllowedService = Literal["api", "celery_worker", "celery_beat"]
ALLOWED_SERVICES: set[str] = {"api", "celery_worker", "celery_beat"}


def _read_tail(path: str, max_lines: int) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            data = f.readlines()
            if not data:
                return ""
            return "".join(data[-max_lines:])
    except FileNotFoundError:
        raise
    except Exception as e:
        raise RuntimeError(str(e))


@router.get("/tail", response_class=PlainTextResponse)
async def tail_logs(
    service: AllowedService = Query("api"),
    lines: int = Query(200, ge=1, le=5000),
) -> PlainTextResponse:
    if service not in ALLOWED_SERVICES:
        raise HTTPException(status_code=400, detail="invalid service")
    log_dir = os.getenv("LOG_DIR", "/app/logs")
    file_path = os.path.join(log_dir, f"{service}.log")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="log file not found")
    try:
        content = _read_tail(file_path, lines)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="log file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to read log: {e}")
    return PlainTextResponse(content)


