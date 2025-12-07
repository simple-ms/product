from typing import List, Dict
from fastapi import HTTPException, status
from sqlalchemy.exc import SQLAlchemyError

from ..models.product import Product
from ..schemas.product import ProductCreate, ProductStockUpdate, ProductResponse
from ..repository import ProductRepository
from ..logger import logger
from ..kafka_producer import publish_product_created, publish_product_updated, publish_product_deleted


class ProductService:
    """Service for product business logic."""
    
    def __init__(self, product_repository: ProductRepository):
        self.product_repository = product_repository
    
    async def create_product(
        self, 
        product_data: ProductCreate, 
        seller_id=None
    ) -> Product:
        """Create a new product."""
        logger.info(f"Creating product: {product_data.name}, price: {product_data.price}, stock: {product_data.stock}, seller: {seller_id}")
        
        try:
            new_product = Product(
                name=product_data.name,
                price=product_data.price,
                stock=product_data.stock,
                seller_id=seller_id  # Track product owner
            )
            product = await self.product_repository.create(new_product)
            logger.info(f"Product created successfully: ID {product.id}, name: {product_data.name}")
            
            # Publish product_created event to Kafka
            publish_product_created({
                "product_id": product.id,
                "seller_id": product.seller_id,
                "name": product.name,
                "price": product.price,
                "stock": product.stock
            })
            
            return product
        except SQLAlchemyError as e:
            await self.product_repository.rollback()
            logger.error(f"Database error while creating product: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def get_product(
        self, 
        product_id: int
    ) -> Product:
        """Get a product by ID."""
        logger.info(f"Fetching product with ID: {product_id}")
        
        try:
            product = await self.product_repository.get_by_id(product_id)
            
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
    
    async def get_all_products(
        self, 
        skip: int = 0, 
        limit: int = 100
    ) -> List[Product]:
        """Get all products with pagination."""
        limit = min(limit, 100)  # Cap the limit
        
        logger.info(f"Fetching products: skip={skip}, limit={limit}")
        
        try:
            products = await self.product_repository.get_all(skip, limit)
            logger.info(f"Retrieved {len(products)} products")
            return products
        except SQLAlchemyError as e:
            logger.error(f"Database error while fetching products: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def update_product(
        self, 
        product_id: int, 
        product_data: ProductCreate, 
        user_id=None
    ) -> Product:
        """Update a product (only owner can update)."""
        logger.info(f"Updating product ID {product_id}")
        
        try:
            product = await self.product_repository.get_by_id(product_id)
            
            if not product:
                logger.warning(f"Product not found: ID {product_id}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Product not found"
                )
            
            # Check ownership if user_id provided
            if user_id and product.seller_id and product.seller_id != user_id:
                logger.warning(f"Unauthorized update attempt by user {user_id} on product {product_id}")
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You can only update your own products"
                )
            
            product.name = product_data.name
            product.price = product_data.price
            product.stock = product_data.stock
            
            updated_product = await self.product_repository.update(product)
            logger.info(f"Product updated successfully: ID {product_id}")
            
            # Publish product_updated event to Kafka
            publish_product_updated({
                "product_id": updated_product.id,
                "seller_id": updated_product.seller_id,
                "name": updated_product.name,
                "price": updated_product.price,
                "stock": updated_product.stock
            })
            
            return updated_product
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.product_repository.rollback()
            logger.error(f"Database error while updating product: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def update_stock(
        self, 
        product_id: int, 
        stock_update: ProductStockUpdate
    ) -> Product:
        """Update product stock (increment or decrement)."""
        logger.info(f"Updating stock for product ID {product_id} by {stock_update.quantity}")
        
        try:
            # Use row lock to prevent race conditions
            product = await self.product_repository.get_by_id_for_update(product_id)
            
            if not product:
                logger.warning(f"Product not found: ID {product_id}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Product not found"
                )
            
            new_stock = product.stock + stock_update.quantity
            
            if new_stock < 0:
                logger.warning(
                    f"Insufficient stock for product {product_id}: "
                    f"current={product.stock}, change={stock_update.quantity}"
                )
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error_code": "INSUFFICIENT_STOCK",
                        "message": "Not enough stock available",
                        "details": {
                            "current_stock": product.stock,
                            "requested_change": stock_update.quantity
                        }
                    }
                )
            
            product.stock = new_stock
            updated_product = await self.product_repository.update(product)
            
            logger.info(
                f"Stock updated for product {product_id}: "
                f"old={product.stock - stock_update.quantity}, new={product.stock}"
            )
            return updated_product
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.product_repository.rollback()
            logger.error(f"Database error while updating product stock: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )
    
    async def delete_product(
        self, 
        product_id: int, 
        user_id=None
    ) -> Dict[str, str]:
        """Delete a product (only owner can delete)."""
        logger.info(f"Deleting product ID {product_id}")
        
        try:
            product = await self.product_repository.get_by_id(product_id)
            
            if not product:
                logger.warning(f"Product not found: ID {product_id}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Product not found"
                )
            
            # Check ownership if user_id provided
            if user_id and product.seller_id and product.seller_id != user_id:
                logger.warning(f"Unauthorized delete attempt by user {user_id} on product {product_id}")
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You can only delete your own products"
                )
            
            # Publish product_deleted event before deleting
            publish_product_deleted(product.id, product.seller_id)
            
            await self.product_repository.delete(product)
            logger.info(f"Product deleted successfully: ID {product_id}")
            return {"message": "Product deleted successfully"}
        except HTTPException:
            raise
        except SQLAlchemyError as e:
            await self.product_repository.rollback()
            logger.error(f"Database error while deleting product: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error occurred"
            )

