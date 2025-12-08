"""
Async Kafka consumer for Product Service.
Listens for stock reservation requests from Order Service using aiokafka.
"""
import json
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select

from ..settings import settings
from ..database import AsyncSessionLocal
from ..models import Product
from .producer import publish_stock_reserved, publish_stock_reservation_failed
from ..logger import logger


async def handle_stock_reservation_request(event_data: dict):
    """
    Handle stock reservation request from Order Service.
    
    This is called when a new order is created and needs stock reservation.
    
    Args:
        event_data: Contains correlation_id, user_id, product_id, quantity
    """
    correlation_id = event_data.get("correlation_id")
    product_id = event_data.get("product_id")
    quantity = event_data.get("quantity")
    user_id = event_data.get("user_id")
    order_id = event_data.get("order_id")
    
    logger.info(
        f"Processing stock reservation request: correlation_id={correlation_id}, "
        f"product_id={product_id}, quantity={quantity}"
    )
    
    async with AsyncSessionLocal() as db:
        try:
            # Get product with row lock
            result = await db.execute(
                select(Product)
                .filter(Product.id == product_id)
                .with_for_update()
            )
            product = result.scalar_one_or_none()
            
            if not product:
                logger.warning(f"Product not found: {product_id}")
                await publish_stock_reservation_failed(
                    {
                        "correlation_id": correlation_id,
                        "order_id": order_id,
                        "user_id": user_id,
                        "product_id": product_id,
                        "quantity": quantity
                    },
                    reason="Product not found"
                )
                return
            
            # Check stock availability
            if product.stock < quantity:
                logger.warning(
                    f"Insufficient stock for product {product_id}: "
                    f"available={product.stock}, requested={quantity}"
                )
                await publish_stock_reservation_failed(
                    {
                        "correlation_id": correlation_id,
                        "order_id": order_id,
                        "user_id": user_id,
                        "product_id": product_id,
                        "quantity": quantity
                    },
                    reason=f"Insufficient stock. Available: {product.stock}, Requested: {quantity}"
                )
                return
            
            # Reserve stock (decrement)
            product.stock -= quantity
            await db.commit()
            
            logger.info(
                f"Stock reserved successfully: product_id={product_id}, "
                f"quantity={quantity}, new_stock={product.stock}"
            )
            
            # Calculate total amount
            total_amount = product.price * quantity
            
            # Publish success event
            await publish_stock_reserved({
                "correlation_id": correlation_id,
                "order_id": order_id,
                "user_id": user_id,
                "product_id": product_id,
                "quantity": quantity,
                "price": product.price,
                "total_amount": total_amount
            })
            
        except Exception as e:
            await db.rollback()
            logger.error(f"Error processing stock reservation: {str(e)}")
            await publish_stock_reservation_failed(
                {
                    "correlation_id": correlation_id,
                    "order_id": order_id,
                    "user_id": user_id,
                    "product_id": product_id,
                    "quantity": quantity
                },
                reason=f"Internal error: {str(e)}"
            )


async def handle_stock_restoration_request(event_data: dict):
    """
    Handle stock restoration request (when order is cancelled).
    
    Args:
        event_data: Contains product_id, quantity
    """
    product_id = event_data.get("product_id")
    quantity = event_data.get("quantity")
    order_id = event_data.get("order_id")
    
    logger.info(
        f"Processing stock restoration request: "
        f"product_id={product_id}, quantity={quantity}, order_id={order_id}"
    )
    
    async with AsyncSessionLocal() as db:
        try:
            result = await db.execute(
                select(Product)
                .filter(Product.id == product_id)
                .with_for_update()
            )
            product = result.scalar_one_or_none()
            
            if product:
                product.stock += quantity
                await db.commit()
                logger.info(
                    f"Stock restored successfully: product_id={product_id}, "
                    f"quantity={quantity}, new_stock={product.stock}"
                )
            else:
                logger.warning(f"Product not found for stock restoration: {product_id}")
                
        except Exception as e:
            await db.rollback()
            logger.error(f"Error restoring stock: {str(e)}")


async def start_stock_event_consumer():
    """Start the async Kafka consumer for stock events."""
    logger.info("Starting Product Service Async Kafka Consumer...")
    
    consumer = AIOKafkaConsumer(
        "stock-reservation-requests",
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    await consumer.start()
    logger.info("Listening for stock reservation requests...")
    
    try:
        async for message in consumer:
            try:
                event_data = message.value
                event_type = event_data.get("event_type")
                
                logger.info(f"Received event: {event_type}")
                
                if event_type == "stock_reservation_request":
                    await handle_stock_reservation_request(event_data)
                elif event_type == "stock_restoration_request":
                    await handle_stock_restoration_request(event_data)
                else:
                    logger.warning(f"Unknown event type: {event_type}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped")


if __name__ == "__main__":
    import asyncio
    asyncio.run(start_stock_event_consumer())
