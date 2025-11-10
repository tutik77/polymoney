from __future__ import annotations

from fastapi import APIRouter
from celery.result import AsyncResult

from ...tasks import (
    ingest_leaderboard_task,
    follow_user_activities_task,
)
from ...celery_app import celery_app
from ..schemas import FollowRequest, IngestOnceRequest


router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/ingest/once")
async def trigger_ingest_once(payload: IngestOnceRequest | None = None) -> dict:
    kwargs = payload.model_dump(exclude_none=True) if payload else {}
    result = ingest_leaderboard_task.apply_async(kwargs=kwargs)
    return {"status": "scheduled", "task_id": result.id}


@router.delete("/ingest/once")
async def stop_ingest_once() -> dict:
    inspector = celery_app.control.inspect()
    active_tasks = inspector.active() or {}
    
    cancelled_tasks = []
    for worker, tasks in active_tasks.items():
        for task in tasks:
            if task.get("name") == "polymoney.ingest_leaderboard":
                celery_app.control.revoke(task["id"], terminate=True)
                cancelled_tasks.append({"task_id": task["id"], "worker": worker})
    
    if cancelled_tasks:
        return {"status": "cancelled", "tasks": cancelled_tasks}
    return {"status": "not_running"}


@router.post("/activities/follow/{address}")
async def start_follow(address: str, payload: FollowRequest | None = None) -> dict:
    display_name = payload.display_name if payload and payload.display_name is not None else None
    poll_interval = payload.poll_interval if payload and payload.poll_interval is not None else None
    bootstrap = payload.bootstrap if payload and payload.bootstrap is not None else True
    
    result = follow_user_activities_task.apply_async(
        kwargs={
            "user_address": address,
            "display_name": display_name,
            "poll_interval": poll_interval,
            "bootstrap": bootstrap,
        }
    )
    return {"status": "started", "task_id": result.id}


@router.delete("/activities/follow/{address}")
async def stop_follow(address: str) -> dict:
    inspector = celery_app.control.inspect()
    active_tasks = inspector.active() or {}
    
    for worker, tasks in active_tasks.items():
        for task in tasks:
            if task.get("name") == "polymoney.follow_user_activities":
                task_args = task.get("kwargs", {})
                if task_args.get("user_address") == address:
                    celery_app.control.revoke(task["id"], terminate=True)
                    return {"status": "cancelled", "task_id": task["id"]}
    
    return {"status": "not_running"}


@router.get("/activities/follow")
async def list_following() -> dict:
    inspector = celery_app.control.inspect()
    active_tasks = inspector.active() or {}
    
    running = {}
    for worker, tasks in active_tasks.items():
        for task in tasks:
            if task.get("name") == "polymoney.follow_user_activities":
                task_args = task.get("kwargs", {})
                address = task_args.get("user_address")
                if address:
                    running[address] = {"task_id": task["id"], "worker": worker}
    
    return {"tasks": running}


@router.get("/tasks/{task_id}")
async def get_task_status(task_id: str) -> dict:
    result = AsyncResult(task_id, app=celery_app)
    return {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
    }



