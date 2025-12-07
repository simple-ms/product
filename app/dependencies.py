from uuid import UUID
from fastapi import Depends, Header, HTTPException, status
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


async def get_current_user_id(
    x_user_id: str = Header(..., alias="X-User-Id")
) -> UUID:
    """
    Extract user ID from X-User-Id header set by Nginx after token validation.
    """
    try:
        return UUID(x_user_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user ID in header"
        )


async def get_current_user_role(
    x_user_role: str = Header(..., alias="X-User-Role")
) -> str:
    """
    Extract user role from X-User-Role header set by Nginx after token validation.
    """
    return x_user_role

