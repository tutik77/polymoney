from __future__ import annotations

from celery import Celery
import os
from datetime import timedelta

from .config import get_settings
from .logging_setup import configure_logging


configure_logging()

settings = get_settings()

celery_app = Celery(
    "polymoney",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    # Redirect stdout/stderr into Celery logs as INFO and don't hijack root logger
    worker_redirect_stdouts=True,
    worker_redirect_stdouts_level="INFO",
    worker_hijack_root_logger=False,
    # Make Celery's own log formatter minimal (keep only our message)
    worker_log_format="%(message)s",
    worker_task_log_format="%(message)s",
)

# Periodic schedule (beat)
try:
    settlement_interval_minutes = int(os.getenv("SETTLEMENT_INTERVAL_MINUTES", "120"))
    force_settle_after_days = int(os.getenv("FORCE_SETTLE_AFTER_DAYS", "2"))
    celery_app.conf.beat_schedule = {
        "settle-positions-all": {
            "task": "polymoney.settle_positions_all",
            "schedule": timedelta(minutes=settlement_interval_minutes),
            "kwargs": {"force_settle_after_days": force_settle_after_days},
        }
    }
except Exception:
    # Fallback: no beat schedule configured
    pass

from . import tasks  # noqa: E402, F401
 
