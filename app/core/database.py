"""
MongoDB database connection and management
"""

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ConnectionFailure
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)


class Database:
    """
    MongoDB database manager
    """
    client: AsyncIOMotorClient = None
    db: AsyncIOMotorDatabase = None


database = Database()


async def connect_to_mongo():
    """
    Establish connection to MongoDB
    Called on application startup
    """
    try:
        logger.info("Connecting to MongoDB...")
        
        database.client = AsyncIOMotorClient(
            settings.MONGODB_URL,
            minPoolSize=settings.MONGODB_MIN_POOL_SIZE,
            maxPoolSize=settings.MONGODB_MAX_POOL_SIZE,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=10000
        )
        
        database.db = database.client[settings.MONGODB_DB_NAME]
        
        # Test connection
        await database.client.admin.command('ping')
        
        logger.info(f"Successfully connected to MongoDB: {settings.MONGODB_DB_NAME}")
        
        # Create indexes
        await create_indexes()
        
    except ConnectionFailure as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to MongoDB: {str(e)}")
        raise


async def close_mongo_connection():
    """
    Close MongoDB connection
    Called on application shutdown
    """
    try:
        if database.client:
            database.client.close()
            logger.info("MongoDB connection closed")
    except Exception as e:
        logger.error(f"Error closing MongoDB connection: {str(e)}")


async def get_database() -> AsyncIOMotorDatabase:
    """
    Get database instance
    Used as dependency in API endpoints
    """
    return database.db


async def create_indexes():
    """
    Create database indexes for better query performance
    """
    try:
        db = database.db
        
        logger.info("Creating database indexes...")
        
        # Integrations indexes
        await db.integrations.create_index([("user_id", 1), ("platform", 1)], unique=True)
        await db.integrations.create_index([("user_id", 1)])
        
        # Ad campaigns indexes
        await db.ad_campaigns.create_index([("user_id", 1), ("date", -1)])
        await db.ad_campaigns.create_index([("user_id", 1), ("platform", 1), ("date", -1)])
        await db.ad_campaigns.create_index([("user_id", 1), ("campaign_id", 1)])
        
        # SEO metrics indexes
        await db.seo_metrics.create_index([("user_id", 1), ("date", -1)])
        await db.seo_metrics.create_index([("user_id", 1), ("domain", 1), ("date", -1)])
        
        # Inbox messages indexes
        await db.inbox_messages.create_index([("user_id", 1), ("timestamp", -1)])
        await db.inbox_messages.create_index([("user_id", 1), ("platform", 1)])
        await db.inbox_messages.create_index([("user_id", 1), ("read", 1)])
        await db.inbox_messages.create_index([("user_id", 1), ("sender.id", 1)])
        
        # Email campaigns indexes
        await db.email_campaigns.create_index([("user_id", 1), ("sent_at", -1)])
        await db.email_campaigns.create_index([("user_id", 1), ("status", 1)])
        await db.email_campaigns.create_index([("user_id", 1), ("campaign_type", 1)])
        
        # Email scheduled indexes
        await db.email_scheduled.create_index([("user_id", 1), ("scheduled_at", 1)])
        await db.email_scheduled.create_index([("user_id", 1), ("schedule_status", 1)])
        
        # Cold calls indexes
        await db.cold_calls.create_index([("user_id", 1), ("called_at", -1)])
        await db.cold_calls.create_index([("user_id", 1), ("agent_id", 1)])
        await db.cold_calls.create_index([("user_id", 1), ("outcome", 1)])
        await db.cold_calls.create_index([("user_id", 1), ("customer_phone", 1)])
        await db.cold_calls.create_index([("user_id", 1), ("follow_up_date", 1)])
        
        # Branding metrics indexes
        await db.branding_metrics.create_index([("user_id", 1), ("date", -1)])
        
        # Social posts indexes
        await db.social_posts.create_index([("user_id", 1), ("posted_at", -1)])
        await db.social_posts.create_index([("user_id", 1), ("platform", 1)])
        
        # Scheduled posts indexes
        await db.scheduled_posts.create_index([("user_id", 1), ("scheduled_at", 1)])
        await db.scheduled_posts.create_index([("user_id", 1), ("status", 1)])
        
        # Brand mentions indexes
        await db.brand_mentions.create_index([("user_id", 1), ("created_at", -1)])
        await db.brand_mentions.create_index([("user_id", 1), ("sentiment", 1)])
        
        # Predictions indexes
        await db.predictions.create_index([("user_id", 1), ("generated_at", -1)])
        await db.predictions.create_index([("user_id", 1), ("type", 1)])
        
        # Recommendations indexes
        await db.recommendations.create_index([("user_id", 1), ("generated_at", -1)])
        await db.recommendations.create_index([("user_id", 1), ("status", 1)])
        await db.recommendations.create_index([("user_id", 1), ("priority", 1)])
        
        logger.info("Database indexes created successfully")
        
    except Exception as e:
        logger.error(f"Error creating indexes: {str(e)}")
        # Don't raise - indexes are optimization, not critical


async def check_database_health() -> bool:
    """
    Check if database connection is healthy
    """
    try:
        await database.client.admin.command('ping')
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {str(e)}")
        return False