"""Centralized configuration for all services."""

import os


class Config:
    """Configuration loaded from environment variables with sensible defaults."""

    # Kafka connection
    KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

    # Topic names
    ORDERS_TOPIC: str = os.getenv("ORDERS_TOPIC", "orders")
    TRADES_TOPIC: str = os.getenv("TRADES_TOPIC", "trades")
    SNAPSHOTS_TOPIC: str = os.getenv("SNAPSHOTS_TOPIC", "snapshots")

    # Connection retry settings
    KAFKA_RETRIES: int = int(os.getenv("KAFKA_RETRIES", "30"))
    KAFKA_RETRY_DELAY: int = int(os.getenv("KAFKA_RETRY_DELAY", "2"))

    # Service URLs
    MATCHER_URL: str = os.getenv("MATCHER_URL", "http://matcher:6000")
    FRONTEND_URL: str = os.getenv("FRONTEND_URL", "http://frontend:5000")

    # Market data settings
    SECURITIES_FILE: str = os.getenv("SECURITIES_FILE", "/app/data/securities.txt")
    ORDER_ID_FILE: str = os.getenv("ORDER_ID_FILE", "/app/data/order_id.txt")

    # Trading simulation
    TICK_SIZE: float = float(os.getenv("TICK_SIZE", "0.05"))
    ORDERS_PER_MIN: int = int(os.getenv("ORDERS_PER_MIN", "8"))
