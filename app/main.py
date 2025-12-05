"""
Main FastAPI application entry point
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from contextlib import asynccontextmanager
from datetime import datetime
import logging
import sys
import time

from app.core.config import settings
from app.core.database import connect_to_mongo, close_mongo_connection, check_database_health
from app.services.cache.redis_service import RedisService
from app.utils.error_handlers import (
    http_exception_handler,
    validation_exception_handler,
    general_exception_handler
)

# Import all routers
from app.api.v1.ads import router as ads_router
from app.api.v1.seo import router as seo_router
from app.api.v1.inbox import router as inbox_router
from app.api.v1.email_marketing import router as email_marketing_router
from app.api.v1.cold_calling import router as cold_calling_router
from app.api.v1.branding import router as branding_router
from app.api.v1.dashboard import router as dashboard_router

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(settings.LOG_FILE) if settings.LOG_FILE else logging.NullHandler()
    ]
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events
    """
    # Startup
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    
    try:
        # Connect to MongoDB
        await connect_to_mongo()
        logger.info("MongoDB connected successfully")
        
        # Connect to Redis (optional - app will continue without it)
        redis_service = RedisService()
        await redis_service.connect()
        
        logger.info("Application startup completed")
        
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        # Only raise if MongoDB fails - Redis is optional
        if "mongo" in str(e).lower() or "database" in str(e).lower():
            raise
        logger.warning("Application starting without some optional services")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    try:
        # Close MongoDB connection
        await close_mongo_connection()
        
        # Close Redis connection
        redis_service = RedisService()
        await redis_service.disconnect()
        
        logger.info("All services disconnected successfully")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")


# OpenAPI Tags Metadata
tags_metadata = [
    {
        "name": "System",
        "description": "System health check and information endpoints",
    },
    {
        "name": "Ads Manager",
        "description": "Manage and analyze advertising campaigns across multiple platforms (Meta, Google, Twitter, TikTok, LinkedIn). View performance metrics, campaign analytics, and get AI-powered recommendations.",
    },
    {
        "name": "SEO",
        "description": "Search Engine Optimization tools and analytics. Track keyword rankings, organic traffic, backlinks, and domain authority across all your websites.",
    },
    {
        "name": "Inbox",
        "description": "Unified inbox for managing messages from all platforms (Facebook Messenger, Instagram, WhatsApp, Twitter, Email). View, reply, and organize conversations in one place.",
    },
    {
        "name": "Email Marketing",
        "description": "Create, manage, and analyze email marketing campaigns. Track open rates, click rates, engagement metrics, and campaign performance.",
    },
    {
        "name": "Cold Calling",
        "description": "Track and analyze cold calling activities. Monitor agent performance, call outcomes, success rates, and get insights on best calling times.",
    },
    {
        "name": "Branding",
        "description": "Monitor your brand presence across social media platforms. Track followers, engagement rates, sentiment, and audience growth.",
    },
    {
        "name": "Dashboard",
        "description": "Unified dashboard combining data from all modules. Get a comprehensive overview of all your marketing and communication channels in one place.",
    },
]

# Create FastAPI application with enhanced OpenAPI documentation
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    ## AI-Powered Multi-Platform CMS Backend API
    
    A comprehensive Content Management System that integrates multiple marketing and communication platforms into a single unified dashboard.
    
    ### Features:
    
    * **Ads Manager** - Manage campaigns across Meta, Google, Twitter, TikTok, and LinkedIn
    * **SEO Tools** - Track rankings, traffic, and optimize your search presence
    * **Unified Inbox** - Manage messages from all platforms in one place
    * **Email Marketing** - Create and analyze email campaigns
    * **Cold Calling** - Track and optimize your sales calls
    * **Branding** - Monitor your brand across social media platforms
    
    ### Authentication:
    
    Most endpoints require authentication. Use the `/auth/login` endpoint to get your access token.
    
    ### Rate Limiting:
    
    API requests are rate-limited to ensure fair usage. Check response headers for rate limit information.
    """,
    summary="Unified CMS for managing all your marketing and communication channels",
    terms_of_service="https://example.com/terms/",
    contact={
        "name": "API Support",
        "url": "https://example.com/support",
        "email": "support@example.com",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
    openapi_tags=tags_metadata,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)


# Add middleware
# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=settings.CORS_ALLOW_CREDENTIALS,
    allow_methods=settings.CORS_ALLOW_METHODS,
    allow_headers=settings.CORS_ALLOW_HEADERS,
)

# GZip compression
app.add_middleware(GZipMiddleware, minimum_size=1000)


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Log all incoming requests
    """
    start_time = time.time()
    
    # Log request
    logger.info(f"Request: {request.method} {request.url.path}")
    
    try:
        response = await call_next(request)
        
        # Calculate processing time
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        
        # Log response
        logger.info(
            f"Response: {request.method} {request.url.path} - "
            f"Status: {response.status_code} - "
            f"Time: {process_time:.3f}s"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Request failed: {request.method} {request.url.path} - Error: {str(e)}")
        raise


# Exception handlers
app.add_exception_handler(StarletteHTTPException, http_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(Exception, general_exception_handler)


# Health check endpoints
@app.get(
    "/",
    tags=["System"],
    summary="Root endpoint",
    description="Get basic information about the API",
    response_description="Application information"
)
async def root():
    """
    Root endpoint - Returns basic API information
    
    Use this endpoint to verify the API is running and get version information.
    """
    return {
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "environment": settings.ENVIRONMENT,
        "status": "running",
        "docs_url": "/docs",
        "redoc_url": "/redoc",
        "openapi_url": "/openapi.json"
    }


@app.get(
    "/health",
    tags=["System"],
    summary="Health check",
    description="Check the health status of the API and its dependencies",
    response_description="Health status of all services"
)
async def health_check():
    """
    Health check endpoint - Monitor API and service status
    
    Returns the health status of:
    - Database (MongoDB) - Required
    - Cache (Redis) - Optional
    - Overall API status
    
    Use this endpoint for monitoring and alerting systems.
    """
    db_healthy = await check_database_health()
    
    redis_service = RedisService()
    redis_healthy = await redis_service.exists("health_check_test")
    
    # App is healthy if database is healthy (Redis is optional)
    status = "healthy" if db_healthy else "unhealthy"
    
    return {
        "status": status,
        "database": "healthy" if db_healthy else "unhealthy",
        "cache": "healthy" if redis_healthy else "unavailable",
        "version": settings.APP_VERSION,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get(
    "/metrics",
    tags=["System"],
    summary="API metrics",
    description="Get basic API metrics and statistics",
    response_description="API metrics information"
)
async def metrics():
    """
    Metrics endpoint - Get API usage statistics
    
    Returns basic metrics about the API. More detailed metrics available in production.
    """
    # TODO: Add proper metrics collection
    return {
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "uptime": "N/A",  # Calculate actual uptime
        "environment": settings.ENVIRONMENT
    }


@app.get(
    "/endpoints",
    tags=["System"],
    summary="List all API endpoints",
    description="Get a comprehensive list of all available API endpoints organized by category",
    response_description="List of all API endpoints"
)
async def list_endpoints():
    """
    List all available API endpoints
    
    Returns a structured list of all endpoints organized by module/category.
    Useful for discovering available functionality.
    """
    endpoints = {
        "system": {
            "base_url": "/",
            "endpoints": [
                {"path": "/", "method": "GET", "description": "Root endpoint - API information"},
                {"path": "/health", "method": "GET", "description": "Health check - Service status"},
                {"path": "/metrics", "method": "GET", "description": "API metrics"},
                {"path": "/endpoints", "method": "GET", "description": "List all endpoints"},
                {"path": "/docs", "method": "GET", "description": "Swagger UI documentation"},
                {"path": "/redoc", "method": "GET", "description": "ReDoc documentation"},
                {"path": "/openapi.json", "method": "GET", "description": "OpenAPI schema"},
            ]
        },
        "ads_manager": {
            "base_url": f"{settings.API_V1_PREFIX}/ads",
            "description": "Manage advertising campaigns across multiple platforms",
            "endpoints": [
                {"path": "/overview", "method": "GET", "description": "Get aggregated ads overview"},
                {"path": "/overview/platforms", "method": "GET", "description": "Get platform-specific overview"},
                {"path": "/analytics/overview", "method": "GET", "description": "Get ads analytics overview"},
                {"path": "/analytics/performance-over-time", "method": "GET", "description": "Get performance trends"},
                {"path": "/campaigns", "method": "GET", "description": "List all campaigns"},
                {"path": "/campaigns/{campaign_id}", "method": "GET", "description": "Get campaign details"},
                {"path": "/recommendations", "method": "GET", "description": "Get AI-powered recommendations"},
                {"path": "/predictions", "method": "GET", "description": "Get performance predictions"},
            ]
        },
        "seo": {
            "base_url": f"{settings.API_V1_PREFIX}/seo",
            "description": "SEO tools and analytics",
            "endpoints": [
                {"path": "/overview", "method": "GET", "description": "Get SEO overview"},
                {"path": "/overview/domains", "method": "GET", "description": "List tracked domains"},
                {"path": "/keywords", "method": "GET", "description": "Get keyword rankings"},
                {"path": "/rankings", "method": "GET", "description": "Get ranking data"},
                {"path": "/traffic", "method": "GET", "description": "Get traffic analytics"},
                {"path": "/competitors", "method": "GET", "description": "Analyze competitors"},
                {"path": "/growth-report", "method": "GET", "description": "Get growth report"},
            ]
        },
        "inbox": {
            "base_url": f"{settings.API_V1_PREFIX}/inbox",
            "description": "Unified inbox for all platforms",
            "endpoints": [
                {"path": "/messages", "method": "GET", "description": "Get messages"},
                {"path": "/messages/{message_id}", "method": "GET", "description": "Get message details"},
                {"path": "/messages/{message_id}/mark-read", "method": "PATCH", "description": "Mark as read"},
                {"path": "/messages/{message_id}/reply", "method": "POST", "description": "Reply to message"},
                {"path": "/messages/{message_id}/archive", "method": "PATCH", "description": "Archive message"},
                {"path": "/stats", "method": "GET", "description": "Get inbox statistics"},
            ]
        },
        "email_marketing": {
            "base_url": f"{settings.API_V1_PREFIX}/email-marketing",
            "description": "Email campaign management",
            "endpoints": [
                {"path": "/campaigns", "method": "GET", "description": "List campaigns"},
                {"path": "/campaigns", "method": "POST", "description": "Create campaign"},
                {"path": "/campaigns/{campaign_id}", "method": "GET", "description": "Get campaign details"},
                {"path": "/campaigns/{campaign_id}", "method": "PATCH", "description": "Update campaign"},
                {"path": "/campaigns/{campaign_id}/send", "method": "POST", "description": "Send campaign"},
                {"path": "/analytics/overview", "method": "GET", "description": "Get analytics overview"},
                {"path": "/analytics/performance-over-time", "method": "GET", "description": "Get performance trends"},
            ]
        },
        "cold_calling": {
            "base_url": f"{settings.API_V1_PREFIX}/cold-calling",
            "description": "Cold calling tracking and analytics",
            "endpoints": [
                {"path": "/overview", "method": "GET", "description": "Get calling overview"},
                {"path": "/stats/realtime", "method": "GET", "description": "Get real-time stats"},
                {"path": "/stats/agents", "method": "GET", "description": "Get agent performance"},
                {"path": "/analytics/performance", "method": "GET", "description": "Get performance analytics"},
                {"path": "/history", "method": "GET", "description": "Get call history"},
            ]
        },
        "branding": {
            "base_url": f"{settings.API_V1_PREFIX}/branding",
            "description": "Brand presence monitoring",
            "endpoints": [
                {"path": "/overview", "method": "GET", "description": "Get branding overview"},
                {"path": "/platforms", "method": "GET", "description": "List connected platforms"},
                {"path": "/growth", "method": "GET", "description": "Get audience growth"},
                {"path": "/engagement-summary", "method": "GET", "description": "Get engagement summary"},
            ]
        },
        "dashboard": {
            "base_url": f"{settings.API_V1_PREFIX}/dashboard",
            "description": "Unified dashboard combining all modules",
            "endpoints": [
                {"path": "/overview", "method": "GET", "description": "Get unified dashboard overview"},
            ]
        }
    }
    
    return {
        "api_version": settings.APP_VERSION,
        "base_url": settings.API_V1_PREFIX,
        "total_modules": len(endpoints) - 1,  # Exclude system
        "endpoints": endpoints,
        "documentation": {
            "swagger_ui": "/docs",
            "redoc": "/redoc",
            "openapi_schema": "/openapi.json"
        }
    }


# Include API routers
app.include_router(
    ads_router,
    prefix=f"{settings.API_V1_PREFIX}/ads",
    tags=["Ads Manager"]
)

app.include_router(
    seo_router,
    prefix=f"{settings.API_V1_PREFIX}/seo",
    tags=["SEO"]
)

app.include_router(
    inbox_router,
    prefix=f"{settings.API_V1_PREFIX}/inbox",
    tags=["Inbox"]
)

app.include_router(
    email_marketing_router,
    prefix=f"{settings.API_V1_PREFIX}/email-marketing",
    tags=["Email Marketing"]
)

app.include_router(
    cold_calling_router,
    prefix=f"{settings.API_V1_PREFIX}/cold-calling",
    tags=["Cold Calling"]
)

app.include_router(
    branding_router,
    prefix=f"{settings.API_V1_PREFIX}/branding",
    tags=["Branding"]
)

app.include_router(
    dashboard_router,
    prefix=f"{settings.API_V1_PREFIX}/dashboard",
    tags=["Dashboard"]
)


# Development-only endpoints
if settings.DEBUG:
    @app.get("/debug/config")
    async def debug_config():
        """
        Debug endpoint to view configuration (development only)
        """
        return {
            "app_name": settings.APP_NAME,
            "environment": settings.ENVIRONMENT,
            "mongodb_db": settings.MONGODB_DB_NAME,
            "redis_db": settings.REDIS_DB,
            "debug_mode": settings.DEBUG
        }


logger.info(f"FastAPI application initialized - Docs available at /docs")