#!/usr/bin/env python3
"""
Database seeding script for product service.
Seeds initial product data into the database.
"""

import asyncio
import sys
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select

# Add parent directory to path to import app modules
sys.path.insert(0, '/app')

from app.models import Product
from app.database import Base
from app.settings import settings


async def seed_products():
    """Seed initial products into the database."""
    # Create engine
    engine = create_async_engine(settings.database_url, echo=True)
    
    # Create session
    AsyncSessionLocal = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    
    async with engine.begin() as conn:
        # Create tables if they don't exist
        await conn.run_sync(Base.metadata.create_all)
    
    async with AsyncSessionLocal() as session:
        # Check if products already exist
        result = await session.execute(select(Product))
        existing_products = result.scalars().all()
        
        if existing_products:
            print(f"Products already exist ({len(existing_products)} products found). Skipping seeding.")
            return
        
        # Seed products
        products = [
            Product(
                name="Laptop Pro 15",
                price=1300,
                stock=25
            ),
            Product(
                name="Wireless Mouse",
                price=30,
                stock=100
            ),
            Product(
                name="Mechanical Keyboard",
                price=90,
                stock=50
            ),
            Product(
                name="USB-C Hub",
                price=50,
                stock=75
            ),
            Product(
                name="Headphones",
                price=200,
                stock=30
            ),
        ]
        
        session.add_all(products)
        await session.commit()
        
        print(f"Successfully seeded {len(products)} products!")
    
    await engine.dispose()


if __name__ == "__main__":
    print("Starting product database seeding...")
    asyncio.run(seed_products())
    print("Seeding complete!")
