"""
Kafka integration package for Product Service.
Provides async Kafka producer and consumer functionality.
"""
from .producer import (
    kafka_producer,
    publish_stock_reserved,
    publish_stock_reservation_failed,
    publish_product_created,
    publish_product_updated,
    publish_product_deleted
)
from .consumer import start_stock_event_consumer

__all__ = [
    "kafka_producer",
    "publish_stock_reserved",
    "publish_stock_reservation_failed",
    "publish_product_created",
    "publish_product_updated",
    "publish_product_deleted",
    "start_stock_event_consumer"
]
