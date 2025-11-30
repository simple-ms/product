from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from .database import get_db, init_db
from .models import Product
from .logger import logger
from .schemas import ProductCreate

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()
    logger.info("Product service started")

@app.post("/product")
def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    logger.info(f"Creating product: {product.name}, price: {product.price}, stock: {product.stock}")
    
    new_product = Product(
        name=product.name,
        price=product.price,
        stock=product.stock
    )
    db.add(new_product)
    db.commit()
    db.refresh(new_product)
    logger.info(f"Product created successfully: ID {new_product.id}, name: {product.name}")
    return {"message": "Product created successfully", "id": new_product.id}

@app.get("/product/{product_id}")
def get_product(product_id: int, db: Session = Depends(get_db)):
    logger.info(f"Fetching product with ID: {product_id}")
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        logger.warning(f"Product not found: ID {product_id}")
        raise HTTPException(status_code=404, detail="Product not found")
    logger.info(f"Product retrieved: ID {product_id}, name: {product.name}")
    return {
        "id": product.id,
        "name": product.name,
        "price": product.price,
        "stock": product.stock
    }