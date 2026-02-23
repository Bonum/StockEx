"""Structured logging utilities for the trading system.

Provides JSON-formatted logging with correlation IDs for tracing
requests across services.
"""
import logging
import json
import time
import uuid
import threading
from functools import wraps

# Thread-local storage for correlation ID
_context = threading.local()


def get_correlation_id():
    """Get current correlation ID, or generate a new one."""
    if not hasattr(_context, 'correlation_id') or _context.correlation_id is None:
        _context.correlation_id = str(uuid.uuid4())[:8]
    return _context.correlation_id


def set_correlation_id(correlation_id):
    """Set correlation ID for current thread."""
    _context.correlation_id = correlation_id


def clear_correlation_id():
    """Clear correlation ID for current thread."""
    _context.correlation_id = None


class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""

    def format(self, record):
        log_entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": get_correlation_id(),
        }

        # Add component if set
        if hasattr(record, 'component'):
            log_entry["component"] = record.component

        # Add extra fields
        if hasattr(record, 'extra_data') and record.extra_data:
            log_entry.update(record.extra_data)

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add source location for errors
        if record.levelno >= logging.ERROR:
            log_entry["source"] = {
                "file": record.filename,
                "line": record.lineno,
                "function": record.funcName
            }

        return json.dumps(log_entry)


def setup_logging(component_name, level=logging.INFO, json_format=True):
    """Configure logging for a service component.

    Args:
        component_name: Name of the service (e.g., "matcher", "dashboard")
        level: Logging level
        json_format: Use JSON formatting (True) or standard format (False)

    Returns:
        Logger instance
    """
    logger = logging.getLogger(component_name)
    logger.setLevel(level)

    # Remove existing handlers
    logger.handlers = []

    # Create console handler
    handler = logging.StreamHandler()
    handler.setLevel(level)

    if json_format:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))

    logger.addHandler(handler)

    return logger


class StructuredLogger:
    """Convenience wrapper for structured logging."""

    def __init__(self, component_name, json_format=True):
        self.logger = setup_logging(component_name, json_format=json_format)
        self.component = component_name

    def _log(self, level, message, **extra):
        """Internal logging method with extra data."""
        record = self.logger.makeRecord(
            self.logger.name, level, "", 0, message, (), None
        )
        record.component = self.component
        record.extra_data = extra if extra else None
        self.logger.handle(record)

    def info(self, message, **extra):
        self._log(logging.INFO, message, **extra)

    def debug(self, message, **extra):
        self._log(logging.DEBUG, message, **extra)

    def warning(self, message, **extra):
        self._log(logging.WARNING, message, **extra)

    def error(self, message, **extra):
        self._log(logging.ERROR, message, **extra)

    def order_received(self, order_id, symbol, side, quantity, price, source=None):
        """Log order received event."""
        self.info("order_received", event="order_received",
                  order_id=order_id, symbol=symbol, side=side,
                  quantity=quantity, price=price, source=source)

    def trade_executed(self, trade_id, symbol, price, quantity, buy_id, sell_id):
        """Log trade execution event."""
        self.info("trade_executed", event="trade_executed",
                  trade_id=trade_id, symbol=symbol, price=price,
                  quantity=quantity, buy_id=buy_id, sell_id=sell_id)

    def order_cancelled(self, order_id, reason=None):
        """Log order cancellation event."""
        self.info("order_cancelled", event="order_cancelled",
                  order_id=order_id, reason=reason)


def with_correlation_id(func):
    """Decorator to set correlation ID for request handling."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Check for correlation ID in headers (for Flask)
        try:
            from flask import request
            correlation_id = request.headers.get('X-Correlation-ID')
            if correlation_id:
                set_correlation_id(correlation_id)
            else:
                set_correlation_id(str(uuid.uuid4())[:8])
        except:
            set_correlation_id(str(uuid.uuid4())[:8])

        try:
            return func(*args, **kwargs)
        finally:
            clear_correlation_id()

    return wrapper
