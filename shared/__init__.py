"""Shared utilities and configuration for Kafka Trading System."""

from .config import Config
from .kafka_utils import create_producer, create_consumer

__all__ = ["Config", "create_producer", "create_consumer"]
