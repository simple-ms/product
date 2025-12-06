#!/usr/bin/env python3
"""
Kafka consumer runner for Product Service.
This script starts the Kafka consumer to listen for stock reservation requests.
"""

import sys
import signal
import logging
from app.kafka_consumer import start_stock_event_consumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Received shutdown signal, stopping consumer...")
    sys.exit(0)


if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting Product Service Kafka Consumer...")
    logger.info("Listening for stock reservation requests...")
    
    try:
        start_stock_event_consumer()
    except Exception as e:
        logger.error(f"Consumer error: {str(e)}")
        sys.exit(1)

