"""
Redis caching service
"""

import redis.asyncio as redis  # type: ignore
from typing import Any, Optional
import json
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)


class RedisService:
    """
    Redis service for caching
    """
    
    _instance = None
    _client: Optional[redis.Redis] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisService, cls).__new__(cls)
        return cls._instance
    
    async def connect(self):
        """
        Connect to Redis (optional - app will continue without Redis if connection fails)
        """
        try:
            if self._client is None:
                # Build connection kwargs
                connection_kwargs = {
                    "db": settings.REDIS_DB,
                    "encoding": "utf-8",
                    "decode_responses": True
                }
                
                # Add password if provided
                if settings.REDIS_PASSWORD:
                    connection_kwargs["password"] = settings.REDIS_PASSWORD
                
                # SSL should be handled via URL scheme (use rediss:// instead of redis://)
                # If REDIS_SSL is True but URL uses redis://, log a warning
                if settings.REDIS_SSL and settings.REDIS_URL.startswith("redis://"):
                    logger.warning(
                        "REDIS_SSL is True but URL uses redis://. "
                        "Consider using rediss:// in REDIS_URL for SSL connections."
                    )
                
                self._client = await redis.from_url(
                    settings.REDIS_URL,
                    **connection_kwargs
                )
                
                # Test connection
                await self._client.ping()
                logger.info("Successfully connected to Redis")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {str(e)}. Caching will be disabled.")
            logger.warning("To enable caching, make sure Redis is running and REDIS_URL is correct.")
            self._client = None  # Set to None so methods know Redis is unavailable
    
    async def disconnect(self):
        """
        Disconnect from Redis
        """
        if self._client:
            await self._client.close()
            logger.info("Redis connection closed")
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache
        """
        try:
            if self._client is None:
                await self.connect()
                if self._client is None:
                    return None  # Redis not available
            
            value = await self._client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.debug(f"Error getting key {key} from Redis: {str(e)}")
            return None
    
    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """
        Set value in cache with TTL (time to live) in seconds
        """
        try:
            if self._client is None:
                await self.connect()
                if self._client is None:
                    return False  # Redis not available
            
            serialized_value = json.dumps(value)
            await self._client.setex(key, ttl, serialized_value)
            return True
        except Exception as e:
            logger.debug(f"Error setting key {key} in Redis: {str(e)}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete key from cache
        """
        try:
            if self._client is None:
                await self.connect()
                if self._client is None:
                    return False  # Redis not available
            
            await self._client.delete(key)
            return True
        except Exception as e:
            logger.debug(f"Error deleting key {key} from Redis: {str(e)}")
            return False
    
    async def delete_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching pattern
        """
        try:
            if self._client is None:
                await self.connect()
                if self._client is None:
                    return 0  # Redis not available
            
            keys = []
            async for key in self._client.scan_iter(match=pattern):
                keys.append(key)
            
            if keys:
                await self._client.delete(*keys)
                logger.info(f"Deleted {len(keys)} keys matching pattern: {pattern}")
                return len(keys)
            
            return 0
        except Exception as e:
            logger.debug(f"Error deleting pattern {pattern} from Redis: {str(e)}")
            return 0
    
    async def exists(self, key: str) -> bool:
        """
        Check if key exists
        """
        try:
            if self._client is None:
                await self.connect()
                if self._client is None:
                    return False  # Redis not available
            
            return await self._client.exists(key) > 0
        except Exception as e:
            logger.debug(f"Error checking existence of key {key}: {str(e)}")
            return False
    
    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment counter
        """
        try:
            if self._client is None:
                await self.connect()
                if self._client is None:
                    return 0  # Redis not available
            
            return await self._client.incrby(key, amount)
        except Exception as e:
            logger.debug(f"Error incrementing key {key}: {str(e)}")
            return 0
    
    async def expire(self, key: str, ttl: int) -> bool:
        """
        Set expiration time for key
        """
        try:
            if self._client is None:
                await self.connect()
                if self._client is None:
                    return False  # Redis not available
            
            return await self._client.expire(key, ttl)
        except Exception as e:
            logger.debug(f"Error setting expiration for key {key}: {str(e)}")
            return False
    
    async def get_ttl(self, key: str) -> int:
        """
        Get remaining TTL for key
        """
        try:
            if self._client is None:
                await self.connect()
                if self._client is None:
                    return -1  # Redis not available
            
            return await self._client.ttl(key)
        except Exception as e:
            logger.debug(f"Error getting TTL for key {key}: {str(e)}")
            return -1