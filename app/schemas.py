from pydantic import BaseModel, Field


class ProductCreate(BaseModel):
    """Schema for creating a product."""
    name: str = Field(..., min_length=1, max_length=255, description="Product name")
    price: float = Field(..., gt=0, description="Product price")
    stock: int = Field(..., ge=0, description="Available stock")


class ProductResponse(BaseModel):
    """Schema for product response."""
    id: int
    name: str
    price: float
    stock: int
    
    class Config:
        from_attributes = True
