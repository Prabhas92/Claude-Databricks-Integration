"""Structured (JSON-lines) logging and step timing."""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str = "lakehouse") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log(logger: logging.Logger, msg: str, level: int = logging.INFO, **fields: Any) -> None:
    logger.log(level, msg, extra={"fields": fields})


@contextmanager
def timed(logger: logging.Logger, step: str, **fields: Any) -> Iterator[dict[str, Any]]:
    """Log start/end of a step with duration. Yields a dict callers can add result fields to."""
    result: dict[str, Any] = {}
    start = time.monotonic()
    log(logger, f"{step} started", step=step, **fields)
    try:
        yield result
    except Exception:
        log(logger, f"{step} failed", logging.ERROR, step=step,
            duration_s=round(time.monotonic() - start, 2), **fields)
        raise
    log(logger, f"{step} finished", step=step,
        duration_s=round(time.monotonic() - start, 2), **fields, **result)
