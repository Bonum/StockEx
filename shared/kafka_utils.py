"""Reusable Kafka producer and consumer utilities."""

import json
import time
import logging
from typing import Optional, List, Union

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

from .config import Config

logger = logging.getLogger(__name__)


def create_producer(
    bootstrap_servers: Optional[str] = None,
    retries: Optional[int] = None,
    delay: Optional[int] = None,
    component_name: str = "Service",
) -> KafkaProducer:
    """
    Create a Kafka producer with retry logic.

    Args:
        bootstrap_servers: Kafka broker address (default: from Config)
        retries: Number of connection attempts (default: from Config)
        delay: Seconds between retries (default: from Config)
        component_name: Name for logging purposes

    Returns:
        Connected KafkaProducer instance

    Raises:
        RuntimeError: If connection fails after all retries
    """
    bootstrap_servers = bootstrap_servers or Config.KAFKA_BOOTSTRAP
    retries = retries if retries is not None else Config.KAFKA_RETRIES
    delay = delay if delay is not None else Config.KAFKA_RETRY_DELAY

    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info(f"{component_name}: Kafka producer connected")
            print(f"{component_name}: Kafka producer connected")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"{component_name}: Kafka not ready, retry {attempt + 1}/{retries}"
            )
            print(f"{component_name}: Kafka not ready, retry {attempt + 1}/{retries}")
            time.sleep(delay)

    raise RuntimeError(f"{component_name}: Cannot connect to Kafka after {retries} attempts")


def create_consumer(
    topics: Union[str, List[str]],
    bootstrap_servers: Optional[str] = None,
    group_id: Optional[str] = None,
    auto_offset_reset: str = "latest",
    retries: Optional[int] = None,
    delay: Optional[int] = None,
    component_name: str = "Service",
) -> KafkaConsumer:
    """
    Create a Kafka consumer with retry logic.

    Args:
        topics: Topic name or list of topic names to subscribe to
        bootstrap_servers: Kafka broker address (default: from Config)
        group_id: Consumer group ID
        auto_offset_reset: Where to start reading ('earliest' or 'latest')
        retries: Number of connection attempts (default: from Config)
        delay: Seconds between retries (default: from Config)
        component_name: Name for logging purposes

    Returns:
        Connected KafkaConsumer instance

    Raises:
        RuntimeError: If connection fails after all retries
    """
    bootstrap_servers = bootstrap_servers or Config.KAFKA_BOOTSTRAP
    retries = retries if retries is not None else Config.KAFKA_RETRIES
    delay = delay if delay is not None else Config.KAFKA_RETRY_DELAY

    # Ensure topics is a list
    if isinstance(topics, str):
        topics = [topics]

    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
            )
            logger.info(f"{component_name}: Kafka consumer connected to {topics}")
            print(f"{component_name}: Kafka consumer connected to {topics}")
            return consumer
        except NoBrokersAvailable:
            logger.warning(
                f"{component_name}: Kafka not ready, retry {attempt + 1}/{retries}"
            )
            print(f"{component_name}: Kafka not ready, retry {attempt + 1}/{retries}")
            time.sleep(delay)

    raise RuntimeError(f"{component_name}: Cannot connect to Kafka after {retries} attempts")
