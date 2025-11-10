from __future__ import annotations

import logging
import logging.handlers
import structlog
import os
from typing import Any
import orjson

from .config import get_settings


class ElasticsearchHandler(logging.Handler):
    def __init__(self, hosts: str) -> None:
        super().__init__()
        self.hosts = hosts.split(",")
        self._client = None
        self._failed = False
    
    def _get_client(self):
        if self._client is None and not self._failed:
            try:
                from elasticsearch import Elasticsearch
                self._client = Elasticsearch(self.hosts, request_timeout=5)
            except Exception as e:
                self._failed = True
                print(f"Failed to initialize Elasticsearch client: {e}")
        return self._client
    
    def emit(self, record: logging.LogRecord) -> None:
        if self._failed:
            return
        try:
            client = self._get_client()
            if client is None:
                return
            
            doc = {
                "timestamp": record.created,
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
            }
            
            if hasattr(record, "event_dict"):
                doc.update(record.event_dict)
            
            client.index(
                index=f"polymoney-logs-{int(record.created // 86400)}",
                document=doc,
            )
        except Exception:
            pass


class StructlogToElasticsearch:
    def __init__(self, hosts: str):
        self.handler = ElasticsearchHandler(hosts)
        self._root_logger = logging.getLogger()
        self._root_logger.addHandler(self.handler)
    
    def __call__(self, logger: Any, method_name: str, event_dict: dict) -> dict:
        record = logging.LogRecord(
            name=logger.name if hasattr(logger, "name") else "structlog",
            level=getattr(logging, event_dict.get("level", "INFO").upper(), logging.INFO),
            pathname="",
            lineno=0,
            msg=event_dict.get("event", ""),
            args=(),
            exc_info=None,
        )
        record.event_dict = event_dict
        self.handler.emit(record)
        return event_dict


def configure_logging() -> None:
    settings = get_settings()
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    # Optional lightweight file logging with rotation
    log_to_file = os.getenv("LOG_TO_FILE", "true").lower() == "true"
    log_dir = os.getenv("LOG_DIR", "/app/logs")
    service_name = os.getenv("SERVICE_NAME", "app")
    max_mb = int(os.getenv("LOG_FILE_MAX_MB", "5"))
    backup_count = int(os.getenv("LOG_FILE_BACKUP_COUNT", "3"))
    file_path = os.path.join(log_dir, f"{service_name}.log")
    file_handler: logging.Handler | None = None
    if log_to_file:
        try:
            os.makedirs(log_dir, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                file_path,
                maxBytes=max_mb * 1024 * 1024,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setLevel(level)
            file_formatter = logging.Formatter("%(message)s")
            file_handler.setFormatter(file_formatter)
        except Exception as e:
            print(f"File logging disabled due to error: {e}")
            file_handler = None

    shared_processors = [
        timestamper,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    final_processors = shared_processors.copy()
    
    if settings.elasticsearch_enabled:
        try:
            final_processors.append(StructlogToElasticsearch(settings.elasticsearch_hosts))
        except Exception as e:
            print(f"Elasticsearch logging disabled due to error: {e}")
    # Emit to file via stdlib if configured
    if file_handler is not None:
        class StructlogToFile:
            def __init__(self, handler: logging.Handler):
                self.handler = handler
                root = logging.getLogger()
                root.addHandler(handler)
            def __call__(self, logger: Any, method_name: str, event_dict: dict) -> dict:
                # Render JSON line for easier parsing
                try:
                    msg = orjson.dumps(event_dict).decode("utf-8")
                except Exception:
                    msg = str(event_dict)
                record = logging.LogRecord(
                    name=logger.name if hasattr(logger, "name") else "structlog",
                    level=getattr(logging, event_dict.get("level", "INFO").upper(), logging.INFO),
                    pathname="",
                    lineno=0,
                    msg=msg,
                    args=(),
                    exc_info=None,
                )
                try:
                    self.handler.emit(record)
                except Exception:
                    pass
                return event_dict
        final_processors.append(StructlogToFile(file_handler))

    final_processors.append(structlog.dev.ConsoleRenderer(colors=False))

    structlog.configure(
        processors=final_processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )

    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.dialects").setLevel(logging.WARNING)
    logging.getLogger("elasticsearch").setLevel(logging.WARNING)
