from sqlalchemy import String, Integer, Float
from sqlalchemy.orm import Mapped, mapped_column
from .database import Base


class Product(Base):
    """Product model for catalog management."""
    
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    price: Mapped[float] = mapped_column(Float)
    stock: Mapped[int] = mapped_column(Integer)
    
    def __repr__(self) -> str:
        return f"<Product(id={self.id}, name={self.name}, price={self.price}, stock={self.stock})>"
