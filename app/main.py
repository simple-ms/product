"""
Product Service - Product Catalog Management Microservice

This is the main entry point for the Product service.
All routes are defined in views/ and registered via routes.py
"""
from fastapi import FastAPI

from .routes import register_routes

app = FastAPI(
    title="Product Service",
    description="Product catalog management microservice",
    version="1.0.0",
    docs_url="/docs/product",
    openapi_url="/openapi.json/product",
    redoc_url="/redoc/product"
)

# NOTE: CORS is handled by nginx gateway - no CORS middleware here

# Register all routes
register_routes(app)
