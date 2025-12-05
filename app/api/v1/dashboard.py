"""
Dashboard API - Unified dashboard endpoint combining all modules
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any
from app.services.aggregators.dashboard_aggregator import DashboardAggregator
from app.services.cache.redis_service import RedisService
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get(
    "/overview",
    summary="Get unified dashboard overview",
    description="""
    Get a comprehensive dashboard overview combining data from all modules.
    
    This endpoint aggregates data from:
    - **Ads Manager**: Total spend, clicks, conversions
    - **SEO**: Organic traffic, keywords, rankings
    - **Inbox**: Message counts, unread messages, reply rates
    - **Email Marketing**: Campaign performance, open rates
    - **Branding**: Followers, engagement rates
    
    Perfect for building a unified dashboard widget that shows key metrics from all your marketing channels.
    """,
    response_description="Unified dashboard data from all modules"
)
async def get_dashboard_overview(
    user_id: str = Query(..., description="Your unique user ID", example="user_12345"),
    date_range: str = Query(
        "last_30_days", 
        description="Time period for the dashboard data",
        example="last_30_days",
        enum=["last_7_days", "last_30_days", "last_90_days"]
    )
) -> Dict[str, Any]:
    """
    Get unified dashboard overview combining data from all modules
    
    Returns:
    - Summary from Ads, SEO, Inbox, Email Marketing, and Branding modules
    - Key metrics across all platforms
    - Quick overview for dashboard widgets
    """
    try:
        cache_key = f"dashboard:overview:{user_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            logger.info(f"Cache hit for dashboard overview: {user_id}")
            return cached_data
        
        aggregator = DashboardAggregator()
        dashboard_data = await aggregator.get_dashboard_overview(user_id, date_range)
        
        # Cache for 5 minutes
        await redis_service.set(cache_key, dashboard_data, ttl=300)
        
        return dashboard_data
        
    except Exception as e:
        logger.error(f"Error fetching dashboard overview: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch dashboard overview: {str(e)}"
        )

