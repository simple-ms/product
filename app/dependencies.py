from fastapi import Depends
from fastapi.security import HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from .database import get_db
from .repository import ProductRepository
from .services import ProductService

security = HTTPBearer()


async def get_product_repository(db: AsyncSession = Depends(get_db)) -> ProductRepository:
    """Dependency to get ProductRepository instance."""
    return ProductRepository(db)


async def get_product_service(
    product_repository: ProductRepository = Depends(get_product_repository)
) -> ProductService:
    """Dependency to get ProductService instance."""
    return ProductService(product_repository)

