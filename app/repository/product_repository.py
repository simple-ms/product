from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..models.product import Product


class ProductRepository:
    """Repository for Product database operations."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_by_id(self, product_id: int) -> Product | None:
        """Get product by ID."""
        result = await self.db.execute(select(Product).filter(Product.id == product_id))
        return result.scalar_one_or_none()
    
    async def get_by_id_for_update(self, product_id: int) -> Product | None:
        """Get product by ID with row lock for update."""
        result = await self.db.execute(
            select(Product)
            .filter(Product.id == product_id)
            .with_for_update()  # Lock the row to prevent race conditions
        )
        return result.scalar_one_or_none()
    
    async def get_all(self, skip: int = 0, limit: int = 100) -> List[Product]:
        """Get all products with pagination."""
        result = await self.db.execute(
            select(Product)
            .order_by(Product.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(result.scalars().all())
    
    async def get_by_seller(self, seller_id, skip: int = 0, limit: int = 100) -> List[Product]:
        """Get all products owned by a seller."""
        result = await self.db.execute(
            select(Product)
            .filter(Product.seller_id == seller_id)
            .order_by(Product.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(result.scalars().all())
    
    async def create(self, product: Product) -> Product:
        """Create a new product."""
        self.db.add(product)
        await self.db.commit()
        await self.db.refresh(product)
        return product
    
    async def update(self, product: Product) -> Product:
        """Update an existing product."""
        await self.db.commit()
        await self.db.refresh(product)
        return product
    
    async def delete(self, product: Product) -> None:
        """Delete a product."""
        await self.db.delete(product)
        await self.db.commit()
    
    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.db.rollback()

