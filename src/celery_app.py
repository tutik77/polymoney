from __future__ import annotations

from datetime import timedelta

from celery import Celery
from kombu import Exchange, Queue

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
    # Memory optimization
    result_expires=3600,  # Results expire after 1 hour
    task_compression="gzip",  # Compress task arguments
    result_compression="gzip",  # Compress task results
)

# Queues & routing: send settlement tasks to dedicated 'settlement' queue
celery_app.conf.task_queues = (
    Queue("default", Exchange("default"), routing_key="default"),
    Queue("settlement", Exchange("settlement"), routing_key="settlement"),
)
celery_app.conf.task_default_queue = "default"
celery_app.conf.task_default_exchange = "default"
celery_app.conf.task_default_routing_key = "default"
celery_app.conf.task_routes = {
    "polymoney.settle_positions": {"queue": "settlement", "routing_key": "settlement"},
    "polymoney.settle_positions_all": {"queue": "settlement", "routing_key": "settlement"},
}

celery_app.conf.beat_schedule = {
    "settle-all-regularly": {
        "task": "polymoney.settle_positions_all",
        "schedule": timedelta(minutes=settings.settlement_interval_minutes),
        "kwargs": {"force_settle_after_days": settings.force_settle_after_days},
        "options": {"queue": "settlement"},
    },
}

from . import tasks  # noqa: E402, F401
 
