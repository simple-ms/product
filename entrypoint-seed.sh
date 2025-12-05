#!/bin/bash
# Entrypoint script for product service
# Runs migrations and seeds data before starting the server

set -e

echo "Running database migrations..."
alembic upgrade head

echo "Seeding database with initial data..."
python -m app.seed_data || echo "Seeding skipped (data may already exist)"

echo "Starting product service..."
exec "$@"
