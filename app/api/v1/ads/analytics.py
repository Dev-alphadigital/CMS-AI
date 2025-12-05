"""
Ads Analytics - Detailed analytics and breakdowns
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.analytics.ads_analytics import AdsAnalytics
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/analytics/performance")
async def get_performance_analytics(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None, description="Filter by platform"),
    campaign_id: Optional[str] = Query(None, description="Filter by campaign"),
    date_range: str = Query("last_30_days"),
    group_by: str = Query("day", description="Group by: day, week, month")
) -> Dict[str, Any]:
    """
    Get detailed performance analytics with time-series data
    
    Returns:
    - Time-series data (spend, clicks, conversions over time)
    - Performance by device, age, gender, location
    - Hour-of-day analysis
    - Day-of-week analysis
    """
    try:
        cache_key = f"ads:analytics:performance:{user_id}:{platform}:{campaign_id}:{date_range}:{group_by}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        analytics = AdsAnalytics()
        
        # Build query filters
        query_filter = {"user_id": user_id}
        if platform:
            query_filter["platform"] = platform
        if campaign_id:
            query_filter["campaign_id"] = campaign_id
        
        # Get date range
        dates = _calculate_date_range(date_range)
        query_filter["metrics.date"] = {
            "$gte": dates['start_date'].strftime("%Y-%m-%d"),
            "$lte": dates['end_date'].strftime("%Y-%m-%d")
        }
        
        # Fetch data from MongoDB
        campaigns_cursor = db.ad_campaigns.find(query_filter)
        campaigns_data = await campaigns_cursor.to_list(length=None)
        
        # Process time-series data
        time_series = analytics.generate_time_series(campaigns_data, group_by)
        
        # Calculate demographics breakdown
        demographics = analytics.calculate_demographics_breakdown(campaigns_data)
        
        # Calculate hour-of-day performance
        hourly_performance = analytics.calculate_hourly_performance(campaigns_data)
        
        # Calculate day-of-week performance
        daily_performance = analytics.calculate_daily_performance(campaigns_data)
        
        response = {
            "user_id": user_id,
            "filters": {
                "platform": platform,
                "campaign_id": campaign_id,
                "date_range": date_range
            },
            "time_series": time_series,
            "demographics": demographics,
            "hourly_performance": hourly_performance,
            "daily_performance": daily_performance,
            "generated_at": datetime.utcnow().isoformat()
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching performance analytics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch performance analytics: {str(e)}"
        )


@router.get("/analytics/roi")
async def get_roi_analysis(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Get ROI (Return on Investment) analysis
    
    Returns:
    - ROAS by campaign
    - Cost per conversion
    - Revenue attribution
    - Profit margins
    """
    try:
        cache_key = f"ads:analytics:roi:{user_id}:{platform}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        analytics = AdsAnalytics()
        
        query_filter = {"user_id": user_id}
        if platform:
            query_filter["platform"] = platform
        
        dates = _calculate_date_range(date_range)
        query_filter["metrics.date"] = {
            "$gte": dates['start_date'].strftime("%Y-%m-%d"),
            "$lte": dates['end_date'].strftime("%Y-%m-%d")
        }
        
        campaigns_cursor = db.ad_campaigns.find(query_filter)
        campaigns_data = await campaigns_cursor.to_list(length=None)
        
        # Calculate ROI metrics
        roi_metrics = analytics.calculate_roi_metrics(campaigns_data)
        
        # Calculate cost per conversion by campaign
        cpc_by_campaign = analytics.calculate_cost_per_conversion_by_campaign(campaigns_data)
        
        # Calculate revenue attribution
        revenue_attribution = analytics.calculate_revenue_attribution(campaigns_data)
        
        response = {
            "user_id": user_id,
            "date_range": {
                "start_date": dates['start_date'].isoformat(),
                "end_date": dates['end_date'].isoformat()
            },
            "roi_summary": roi_metrics,
            "cost_per_conversion_by_campaign": cpc_by_campaign,
            "revenue_attribution": revenue_attribution,
            "generated_at": datetime.utcnow().isoformat()
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching ROI analysis: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch ROI analysis: {str(e)}"
        )


@router.get("/analytics/comparison")
async def get_platform_comparison(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Compare performance across all platforms
    
    Returns:
    - Side-by-side platform comparison
    - Best/worst performing platforms
    - Platform efficiency scores
    """
    try:
        cache_key = f"ads:analytics:comparison:{user_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        analytics = AdsAnalytics()
        
        dates = _calculate_date_range(date_range)
        
        # Get data for all platforms
        query_filter = {
            "user_id": user_id,
            "metrics.date": {
                "$gte": dates['start_date'].strftime("%Y-%m-%d"),
                "$lte": dates['end_date'].strftime("%Y-%m-%d")
            }
        }
        
        campaigns_cursor = db.ad_campaigns.find(query_filter)
        campaigns_data = await campaigns_cursor.to_list(length=None)
        
        # Group by platform
        platform_comparison = analytics.compare_platforms(campaigns_data)
        
        # Calculate efficiency scores
        efficiency_scores = analytics.calculate_platform_efficiency(campaigns_data)
        
        # Identify best/worst performers
        best_worst = analytics.identify_best_worst_platforms(platform_comparison)
        
        response = {
            "user_id": user_id,
            "date_range": {
                "start_date": dates['start_date'].isoformat(),
                "end_date": dates['end_date'].isoformat()
            },
            "platform_comparison": platform_comparison,
            "efficiency_scores": efficiency_scores,
            "best_performer": best_worst['best'],
            "worst_performer": best_worst['worst'],
            "generated_at": datetime.utcnow().isoformat()
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching platform comparison: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch platform comparison: {str(e)}"
        )


def _calculate_date_range(date_range: str) -> Dict[str, datetime]:
    """Calculate start and end dates"""
    end = datetime.utcnow()
    
    if date_range == "last_7_days":
        start = end - timedelta(days=7)
    elif date_range == "last_30_days":
        start = end - timedelta(days=30)
    elif date_range == "last_90_days":
        start = end - timedelta(days=90)
    else:
        start = end - timedelta(days=30)
    
    return {"start_date": start, "end_date": end}