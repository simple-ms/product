from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from .database import get_db, init_db
from . import models

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()

class ProductCreate(BaseModel):
    name: str
    price: float
    stock: int

@app.post("/product")
def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    
    new_product = models.Product(
        name=product.name,
        price=product.price,
        stock=product.stock
    )
    db.add(new_product)
    db.commit()
    db.refresh(new_product)
    return {"message": "Product created successfully", "id": new_product.id}

@app.get("/product/{product_id}")
def get_product(product_id: int, db: Session = Depends(get_db)):
    product = db.query(models.Product).filter(models.Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return {
        "id": product.id,
        "name": product.name,
        "price": product.price,
        "stock": product.stock
    }