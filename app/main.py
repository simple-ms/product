from typing import List
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select

from .database import get_db
from .models import Product
from .logger import logger
from .schemas import ProductCreate, ProductResponse

security = HTTPBearer()

app = FastAPI(
    title="Product Service",
    description="Product catalog management microservice",
    version="1.0.0",
    docs_url="/docs/product",
    openapi_url="/openapi.json/product",
    redoc_url="/redoc/product"
)


# --- HEALTH CHECK ---

@app.get("/product/health", tags=["Health"])
async def health_check():
    """Health check endpoint for monitoring."""
    return {"status": "healthy", "service": "product-service"}


# --- PRODUCT ENDPOINTS ---

@app.post(
    "/product",
    response_model=ProductResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Products"]
)
async def create_product(
    product: ProductCreate,
    db: AsyncSession = Depends(get_db),
    token: HTTPBearer = Depends(security)
):
    """
    Create a new product.
    
    Requires authentication.
    """
    logger.info(f"Creating product: {product.name}, price: {product.price}, stock: {product.stock}")
    
    try:
        new_product = Product(
            name=product.name,
            price=product.price,
            stock=product.stock
        )
        db.add(new_product)
        await db.commit()
        await db.refresh(new_product)
        
        logger.info(f"Product created successfully: ID {new_product.id}, name: {product.name}")
        return new_product
        
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Database error while creating product: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )


@app.get(
    "/product/{product_id}",
    response_model=ProductResponse,
    tags=["Products"]
)
async def get_product(
    product_id: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Get a product by ID.
    
    Public endpoint - no authentication required.
    """
    logger.info(f"Fetching product with ID: {product_id}")
    
    try:
        result = await db.execute(select(Product).filter(Product.id == product_id))
        product = result.scalar_one_or_none()
        
        if not product:
            logger.warning(f"Product not found: ID {product_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )
        
        logger.info(f"Product retrieved: ID {product_id}, name: {product.name}")
        return product
        
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching product: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )


@app.get(
    "/products",
    response_model=List[ProductResponse],
    tags=["Products"]
)
async def get_all_products(
    db: AsyncSession = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """
    Get all products with pagination.
    
    Public endpoint - no authentication required.
    """
    logger.info(f"Fetching products: skip={skip}, limit={limit}")
    
    try:
        result = await db.execute(select(Product).offset(skip).limit(limit))
        products = result.scalars().all()
        
        logger.info(f"Retrieved {len(products)} products")
        return products
        
    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching products: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )


@app.put(
    "/product/{product_id}",
    response_model=ProductResponse,
    tags=["Products"]
)
async def update_product(
    product_id: int,
    product_update: ProductCreate,
    db: AsyncSession = Depends(get_db),
    token: HTTPBearer = Depends(security)
):
    """
    Update a product.
    
    Requires authentication.
    """
    logger.info(f"Updating product ID {product_id}")
    
    try:
        result = await db.execute(select(Product).filter(Product.id == product_id))
        product = result.scalar_one_or_none()
        
        if not product:
            logger.warning(f"Product not found: ID {product_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )
        
        # Update fields
        product.name = product_update.name
        product.price = product_update.price
        product.stock = product_update.stock
        
        await db.commit()
        await db.refresh(product)
        
        logger.info(f"Product updated successfully: ID {product_id}")
        return product
        
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Database error while updating product: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )


@app.delete(
    "/product/{product_id}",
    status_code=status.HTTP_200_OK,
    tags=["Products"]
)
async def delete_product(
    product_id: int,
    db: AsyncSession = Depends(get_db),
    token: HTTPBearer = Depends(security)
):
    """
    Delete a product.
    
    Requires authentication.
    """
    logger.info(f"Deleting product ID {product_id}")
    
    try:
        result = await db.execute(select(Product).filter(Product.id == product_id))
        product = result.scalar_one_or_none()
        
        if not product:
            logger.warning(f"Product not found: ID {product_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found"
            )
        
        await db.delete(product)
        await db.commit()
        
        logger.info(f"Product deleted successfully: ID {product_id}")
        return {"message": "Product deleted successfully"}
        
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Database error while deleting product: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error occurred"
        )