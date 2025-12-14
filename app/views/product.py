from typing import List
from uuid import UUID
from fastapi import APIRouter, Depends, status

from ..schemas.product import ProductCreate, ProductStockUpdate, ProductResponse
from ..services import ProductService
from ..dependencies import get_product_service, get_current_user_id, get_current_user_role

router = APIRouter(tags=["Products"])


@router.post(
    "/product",
    response_model=ProductResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_product(
    product: ProductCreate,
    user_id: UUID = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role),
    product_service: ProductService = Depends(get_product_service)
):
    """
    Create a new product.
    
    Only sellers can create products. Buyers must create a seller account.
    Product will be linked to the creating user (seller).
    """
    # Validate user role - only sellers can create products
    if user_role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only sellers can create products. Please create a seller account."
        )
    
    return await product_service.create_product(product, seller_id=user_id)


@router.get(
    "/product/{product_id}",
    response_model=ProductResponse
)
async def get_product(
    product_id: int,
    product_service: ProductService = Depends(get_product_service)
):
    """
    Get a product by ID.
    
    Public endpoint - no authentication required.
    """
    return await product_service.get_product(product_id)


@router.get(
    "/products",
    response_model=List[ProductResponse]
)
async def get_all_products(
    skip: int = 0,
    limit: int = 100,
    product_service: ProductService = Depends(get_product_service)
):
    """
    Get all products with pagination.
    
    Public endpoint - no authentication required.
    """
    return await product_service.get_all_products(skip, limit)


@router.put(
    "/product/{product_id}",
    response_model=ProductResponse
)
async def update_product(
    product_id: int,
    product_update: ProductCreate,
    user_id: UUID = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role),
    product_service: ProductService = Depends(get_product_service)
):
    """
    Update a product.
    
    Only sellers can update products. Only the product owner can update their own products.
    """
    # Validate user role - only sellers can update products
    if user_role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only sellers can update products."
        )
    
    return await product_service.update_product(product_id, product_update, user_id=user_id)


@router.patch(
    "/product/{product_id}/stock",
    response_model=ProductResponse
)
async def update_product_stock(
    product_id: int,
    stock_update: ProductStockUpdate,
    product_service: ProductService = Depends(get_product_service)
):
    """
    Update product stock (increment or decrement).
    
    This endpoint is used internally by the Order service to decrement stock
    when an order is placed, or to increment stock when an order is cancelled.
    
    Args:
        product_id: The product ID
        stock_update: Contains quantity to add (positive) or remove (negative)
    
    Returns:
        Updated product with new stock level
    """
    return await product_service.update_stock(product_id, stock_update)


@router.delete(
    "/product/{product_id}",
    status_code=status.HTTP_200_OK
)
async def delete_product(
    product_id: int,
    user_id: UUID = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role),
    product_service: ProductService = Depends(get_product_service)
):
    """
    Delete a product.
    
    Only sellers can delete products. Only the product owner can delete their own products.
    """
    # Validate user role - only sellers can delete products
    if user_role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only sellers can delete products."
        )
    
    return await product_service.delete_product(product_id, user_id=user_id)


@router.get(
    "/products/me",
    response_model=List[ProductResponse]
)
async def get_my_products(
    skip: int = 0,
    limit: int = 100,
    user_id: UUID = Depends(get_current_user_id),
    user_role: str = Depends(get_current_user_role),
    product_service: ProductService = Depends(get_product_service)
):
    """
    Get all products owned by the current seller.
    
    Requires authentication. Only sellers can access this endpoint.
    """
    from fastapi import HTTPException
    
    # Validate user role - only sellers have products
    if user_role != "seller":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only sellers can view their products."
        )
    
    from ..dependencies import get_product_repository
    
    # Get repository and fetch seller products
    async for repo in get_product_repository():
        products = await repo.get_by_seller(user_id, skip, limit)
        return products


@router.get(
    "/products/seller/{seller_id}",
    response_model=List[ProductResponse]
)
async def get_seller_products(
    seller_id: str,
    skip: int = 0,
    limit: int = 100,
    product_service: ProductService = Depends(get_product_service)
):
    """
    Get all products owned by a specific seller.
    
    Public endpoint - used by order service to fetch seller's products.
    """
    from uuid import UUID
    from ..repository import ProductRepository
    from ..dependencies import get_product_repository
    
    # Get repository and fetch seller products
    async for repo in get_product_repository():
        products = await repo.get_by_seller(UUID(seller_id), skip, limit)
        return products

