"""
Ads Overview - Aggregated view across all platforms
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.aggregators.ads_aggregator import AdsAggregator
from app.services.analytics.ads_analytics import AdsAnalytics
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/overview")
async def get_ads_overview(
    user_id: str = Query(..., description="User ID"),
    date_range: str = Query("last_7_days", description="Date range: last_7_days, last_30_days, last_90_days, custom"),
    start_date: Optional[str] = Query(None, description="Start date for custom range (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for custom range (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """
    Get aggregated ads overview across all connected platforms
    
    Returns:
    - Total spend, impressions, clicks, conversions
    - Platform-wise breakdown
    - Key metrics: CTR, CPC, ROAS, Conversion Rate
    - Trend data (comparison with previous period)
    """
    try:
        # Check Redis cache first
        cache_key = f"ads:overview:{user_id}:{date_range}:{start_date}:{end_date}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            logger.info(f"Cache hit for ads overview: {user_id}")
            return cached_data
        
        logger.info(f"Cache miss for ads overview: {user_id}")
        
        # Calculate date range
        dates = _calculate_date_range(date_range, start_date, end_date)
        
        # Get aggregated data from all platforms
        aggregator = AdsAggregator()
        aggregated_data = await aggregator.aggregate_all_platforms(
            user_id=user_id,
            start_date=dates['start_date'],
            end_date=dates['end_date']
        )
        
        # Calculate analytics
        analytics = AdsAnalytics()
        metrics = analytics.calculate_metrics(aggregated_data)
        
        # Get previous period for comparison
        previous_period_data = await aggregator.aggregate_all_platforms(
            user_id=user_id,
            start_date=dates['previous_start_date'],
            end_date=dates['previous_end_date']
        )
        previous_metrics = analytics.calculate_metrics(previous_period_data)
        
        # Calculate trends
        trends = analytics.calculate_trends(metrics, previous_metrics)
        
        response = {
            "user_id": user_id,
            "date_range": {
                "start_date": dates['start_date'].isoformat(),
                "end_date": dates['end_date'].isoformat(),
                "label": date_range
            },
            "summary": {
                "total_spend": metrics['total_spend'],
                "total_impressions": metrics['total_impressions'],
                "total_clicks": metrics['total_clicks'],
                "total_conversions": metrics['total_conversions'],
                "avg_ctr": metrics['avg_ctr'],
                "avg_cpc": metrics['avg_cpc'],
                "avg_roas": metrics['avg_roas'],
                "conversion_rate": metrics['conversion_rate']
            },
            "trends": {
                "spend_change": trends['spend_change'],
                "clicks_change": trends['clicks_change'],
                "conversions_change": trends['conversions_change'],
                "roas_change": trends['roas_change']
            },
            "platform_breakdown": aggregated_data['platform_breakdown'],
            "top_performing_campaigns": aggregated_data['top_campaigns'][:5],
            "last_updated": datetime.utcnow().isoformat()
        }
        
        # Cache for 2 hours
        await redis_service.set(cache_key, response, ttl=7200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching ads overview: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch ads overview: {str(e)}"
        )


@router.get("/overview/platforms")
async def get_platform_overview(
    user_id: str = Query(..., description="User ID"),
    platform: str = Query(..., description="Platform: meta_ads, google_ads, twitter_ads, tiktok_ads, linkedin_ads"),
    date_range: str = Query("last_7_days")
) -> Dict[str, Any]:
    """
    Get overview for a specific platform
    """
    try:
        cache_key = f"ads:overview:platform:{user_id}:{platform}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        dates = _calculate_date_range(date_range)
        
        aggregator = AdsAggregator()
        platform_data = await aggregator.aggregate_single_platform(
            user_id=user_id,
            platform=platform,
            start_date=dates['start_date'],
            end_date=dates['end_date']
        )
        
        analytics = AdsAnalytics()
        metrics = analytics.calculate_metrics(platform_data)
        
        response = {
            "user_id": user_id,
            "platform": platform,
            "date_range": {
                "start_date": dates['start_date'].isoformat(),
                "end_date": dates['end_date'].isoformat()
            },
            "metrics": metrics,
            "campaigns": platform_data['campaigns'],
            "daily_breakdown": platform_data['daily_data']
        }
        
        await redis_service.set(cache_key, response, ttl=7200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching platform overview: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch platform overview: {str(e)}"
        )


def _calculate_date_range(date_range: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, datetime]:
    """
    Calculate start and end dates based on date_range parameter
    """
    end = datetime.utcnow()
    
    if date_range == "custom" and start_date and end_date:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
    elif date_range == "last_7_days":
        start = end - timedelta(days=7)
    elif date_range == "last_30_days":
        start = end - timedelta(days=30)
    elif date_range == "last_90_days":
        start = end - timedelta(days=90)
    else:
        start = end - timedelta(days=7)  # Default
    
    # Calculate previous period for comparison
    period_length = (end - start).days
    previous_end = start
    previous_start = previous_end - timedelta(days=period_length)
    
    return {
        "start_date": start,
        "end_date": end,
        "previous_start_date": previous_start,
        "previous_end_date": previous_end
    }