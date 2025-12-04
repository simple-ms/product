from pydantic import BaseModel, Field, field_validator


class ProductCreate(BaseModel):
    """Schema for creating a product."""
    name: str = Field(..., min_length=1, max_length=255, description="Product name")
    price: float = Field(..., gt=0, le=1000000, description="Product price (max $1,000,000)")
    stock: int = Field(..., ge=0, le=1000000, description="Available stock (max 1,000,000)")
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate and sanitize product name."""
        v = v.strip()
        if not v:
            raise ValueError('Product name cannot be empty')
        return v
    
    @field_validator('price')
    @classmethod
    def validate_price(cls, v: float) -> float:
        """Validate and round price to 2 decimal places."""
        if v <= 0:
            raise ValueError('Price must be greater than 0')
        return round(v, 2)


class ProductStockUpdate(BaseModel):
    """Schema for updating product stock."""
    quantity: int = Field(..., description="Quantity to add (positive) or remove (negative)")
    
    @field_validator('quantity')
    @classmethod
    def validate_quantity_change(cls, v: int) -> int:
        """Validate quantity change is reasonable."""
        if abs(v) > 100000:
            raise ValueError('Stock change cannot exceed 100,000 units')
        return v


class ProductResponse(BaseModel):
    """Schema for product response."""
    id: int
    name: str
    price: float
    stock: int
    
    class Config:
        from_attributes = True
