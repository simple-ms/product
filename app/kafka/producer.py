"""
Async Kafka producer for Product Service.
Publishes stock reservation responses and product lifecycle events using aiokafka.
"""
import json
from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from ..settings import settings
from ..logger import logger


class AsyncKafkaProducerClient:
    """Async Kafka producer client for publishing stock events."""
    
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self._started = False
    
    async def start(self):
        """Initialize and start Kafka producer connection."""
        if self._started:
            return
        
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                enable_idempotence=True,
                api_version="2.5.0"
            )
            await self.producer.start()
            self._started = True
            logger.info(f"Async Kafka producer connected to {settings.KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to start async Kafka producer: {str(e)}")
            self.producer = None
            self._started = False
    
    async def stop(self):
        """Stop Kafka producer connection."""
        if self.producer and self._started:
            await self.producer.stop()
            self._started = False
            logger.info("Async Kafka producer stopped")
    
    async def send_event(self, topic: str, event_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Send an event to Kafka topic."""
        if not self.producer or not self._started:
            await self.start()
        
        if not self.producer:
            logger.error("Kafka producer not initialized")
            return False
        
        try:
            metadata = await self.producer.send_and_wait(topic, value=event_data, key=key)
            logger.info(
                f"Event sent to topic '{topic}': "
                f"partition={metadata.partition}, offset={metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event to topic '{topic}': {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending event: {str(e)}")
            return False


# Global producer instance
kafka_producer = AsyncKafkaProducerClient()


async def publish_stock_reserved(reservation_data: Dict[str, Any]) -> bool:
    """
    Publish stock_reserved event when stock is successfully reserved.
    
    Args:
        reservation_data: Contains order_id, product_id, quantity, price, etc.
    """
    event = {
        "event_type": "stock_reserved",
        "correlation_id": reservation_data["correlation_id"],
        "order_id": reservation_data["order_id"],
        "user_id": reservation_data["user_id"],
        "product_id": reservation_data["product_id"],
        "quantity": reservation_data["quantity"],
        "price": reservation_data["price"],
        "total_amount": reservation_data["total_amount"]
    }
    return await kafka_producer.send_event(
        topic="stock-events",
        event_data=event,
        key=reservation_data["correlation_id"]
    )


async def publish_stock_reservation_failed(reservation_data: Dict[str, Any], reason: str) -> bool:
    """
    Publish stock_reservation_failed event when stock reservation fails.
    
    Args:
        reservation_data: Contains order_id, product_id, quantity, etc.
        reason: Failure reason
    """
    event = {
        "event_type": "stock_reservation_failed",
        "correlation_id": reservation_data["correlation_id"],
        "order_id": reservation_data.get("order_id"),
        "user_id": reservation_data["user_id"],
        "product_id": reservation_data["product_id"],
        "quantity": reservation_data["quantity"],
        "reason": reason
    }
    return await kafka_producer.send_event(
        topic="stock-events",
        event_data=event,
        key=reservation_data["correlation_id"]
    )


# --- Product Lifecycle Events ---

async def publish_product_created(product_data: dict) -> bool:
    """Publish product_created event when a new product is created."""
    event = {
        "event_type": "product_created",
        "product_id": product_data["product_id"],
        "seller_id": str(product_data["seller_id"]),
        "name": product_data["name"],
        "price": product_data["price"],
        "stock": product_data["stock"]
    }
    logger.info(f"Publishing product_created event for product {product_data['product_id']}")
    return await kafka_producer.send_event(
        topic="product-events",
        event_data=event,
        key=str(product_data["product_id"])
    )


async def publish_product_updated(product_data: dict) -> bool:
    """Publish product_updated event when a product is updated."""
    event = {
        "event_type": "product_updated",
        "product_id": product_data["product_id"],
        "seller_id": str(product_data["seller_id"]),
        "name": product_data["name"],
        "price": product_data["price"],
        "stock": product_data["stock"]
    }
    logger.info(f"Publishing product_updated event for product {product_data['product_id']}")
    return await kafka_producer.send_event(
        topic="product-events",
        event_data=event,
        key=str(product_data["product_id"])
    )


async def publish_product_deleted(product_id: int, seller_id: str) -> bool:
    """Publish product_deleted event when a product is deleted."""
    event = {
        "event_type": "product_deleted",
        "product_id": product_id,
        "seller_id": str(seller_id)
    }
    logger.info(f"Publishing product_deleted event for product {product_id}")
    return await kafka_producer.send_event(
        topic="product-events",
        event_data=event,
        key=str(product_id)
    )
