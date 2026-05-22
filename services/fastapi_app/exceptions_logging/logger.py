###########################################################
# logging_config.py
#
# Simple structured logging (JSON) with 2 log files:
# - /shared_logs/app.log   -> INFO and above
# - /shared_logs/error.log -> ERROR and above
#
# Also prints logs to console (stdout) in JSON format.
###########################################################

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logs.
    If message is already JSON -> merge it into the base payload.
    Otherwise -> store the text as "msg".
    """

    def format(self, record: logging.LogRecord) -> str:
        base = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
        }

        msg = record.getMessage()
        try:
            payload = json.loads(msg)
            if not isinstance(payload, dict):
                payload = {"msg": msg}
        except json.JSONDecodeError:
            payload = {"msg": msg}

        return json.dumps({**base, **payload}, ensure_ascii=False)


def setup_root_logger() -> logging.Logger:
    """
    Initialize root application logger (only once).
    Handlers:
      - Console: INFO+
      - app.log: INFO+
      - error.log: ERROR+
    """
    os.makedirs("/shared/logs", exist_ok=True)

    logger = logging.getLogger("app")
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers on reload
    if logger.handlers:
        return logger

    fmt = JSONFormatter()

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    logger.addHandler(console)

    # Main rotating file (INFO+)
    app_file = RotatingFileHandler(
        "/shared/logs/app.log",
        maxBytes=10_000_000,  # 10MB
        backupCount=10,
        encoding="utf-8",
    )
    app_file.setLevel(logging.INFO)
    app_file.setFormatter(fmt)
    logger.addHandler(app_file)

    # Error-only rotating file (ERROR+)
    err_file = RotatingFileHandler(
        "/shared/logs/error.log",
        maxBytes=5_000_000,  # 5MB
        backupCount=10,
        encoding="utf-8",
    )
    err_file.setLevel(logging.ERROR)
    err_file.setFormatter(fmt)
    logger.addHandler(err_file)

    return logger


ROOT_LOGGER = setup_root_logger()


@dataclass
class AppLogger:
    """
    Helper to log JSON with common fields.
    """
    component: str
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("app"))

    def _log(self, level: str, message: str, exc_info: bool = False, **fields):
        trace_id = fields.pop("trace_id", str(uuid.uuid4()))

        payload = {
            "trace_id": trace_id,
            "component": self.component,
            "msg": message,
            **fields,
        }

        log_fn = getattr(self.logger, level, self.logger.info)
        log_fn(json.dumps(payload, ensure_ascii=False), exc_info=exc_info)

    def info(self, message: str, **fields):
        self._log("info", message, **fields)

    def debug(self, message: str, **fields):
        self._log("debug", message, **fields)

    def warning(self, message: str, **fields):
        self._log("warning", message, **fields)

    def error(self, message: str, **fields):
        self._log("error", message, **fields)

    def exception(self, message: str, **fields):
        self._log("error", message, exc_info=True, **fields)