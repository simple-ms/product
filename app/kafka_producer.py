"""
Kafka producer for Product Service.
Publishes stock reservation responses.
"""
import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from .settings import settings


logger = logging.getLogger("product-service")


class KafkaProducerClient:
    """Kafka producer client for publishing stock events."""
    
    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self._connect()
    
    def _connect(self):
        """Initialize Kafka producer connection."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Kafka producer connected to {settings.KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to connect Kafka producer: {str(e)}")
            self.producer = None
    
    def send_event(self, topic: str, event_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Send an event to Kafka topic."""
        if not self.producer:
            logger.error("Kafka producer not initialized")
            return False
        
        try:
            future = self.producer.send(topic, value=event_data, key=key)
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Event sent to topic '{topic}': "
                f"partition={record_metadata.partition}, offset={record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event to topic '{topic}': {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending event: {str(e)}")
            return False
    
    def close(self):
        """Close Kafka producer connection."""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


# Global producer instance
kafka_producer = KafkaProducerClient()


def publish_stock_reserved(reservation_data: Dict[str, Any]) -> bool:
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
    return kafka_producer.send_event(
        topic="stock-events",
        event_data=event,
        key=reservation_data["correlation_id"]
    )


def publish_stock_reservation_failed(reservation_data: Dict[str, Any], reason: str) -> bool:
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
    return kafka_producer.send_event(
        topic="stock-events",
        event_data=event,
        key=reservation_data["correlation_id"]
    )

