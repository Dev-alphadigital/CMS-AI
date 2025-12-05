"""
FastAPI dependencies for dependency injection
"""

from fastapi import Depends, HTTPException, status
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from motor.motor_asyncio import AsyncIOMotorDatabase
import logging

logger = logging.getLogger(__name__)


async def get_db() -> AsyncIOMotorDatabase:
    """
    Database dependency
    """
    db = await get_database()
    if db is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection not available"
        )
    return db


def get_redis() -> RedisService:
    """
    Redis service dependency
    """
    try:
        return RedisService()
    except Exception as e:
        logger.error(f"Failed to get Redis service: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cache service not available"
        )


# Add more dependencies as needed
# For example: authentication, rate limiting, etc.