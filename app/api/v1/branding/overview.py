"""
Branding Overview - High-level brand metrics across all platforms
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.aggregators.branding_aggregator import BrandingAggregator
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/overview")
async def get_branding_overview(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days", description="Date range: last_7_days, last_30_days, last_90_days")
) -> Dict[str, Any]:
    """
    Get comprehensive branding overview across all social platforms
    
    Returns:
    - Total followers/audience across platforms
    - Engagement metrics (likes, comments, shares)
    - Post performance summary
    - Platform breakdown
    - Brand sentiment score
    - Growth trends
    """
    try:
        cache_key = f"branding:overview:{user_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            logger.info(f"Cache hit for branding overview: {user_id}")
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        # Build query filter
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": dates['start_date'].strftime("%Y-%m-%d"),
                "$lte": dates['end_date'].strftime("%Y-%m-%d")
            }
        }
        
        # Fetch branding metrics
        branding_cursor = db.branding_metrics.find(query_filter).sort("date", -1)
        branding_data = await branding_cursor.to_list(length=None)
        
        if not branding_data:
            return {
                "user_id": user_id,
                "date_range": date_range,
                "message": "No branding data available. Please connect social media accounts.",
                "summary": {}
            }
        
        # Get latest metrics (most recent date)
        latest_metrics = branding_data[0]
        
        # Aggregate platform data
        platforms = latest_metrics.get('platforms', {})
        
        total_followers = 0
        total_engagement = 0
        total_posts = 0
        
        platform_breakdown = []
        
        for platform_name, platform_data in platforms.items():
            followers = platform_data.get('followers', 0)
            engagement_rate = platform_data.get('engagement_rate', 0)
            posts = platform_data.get('posts', 0)
            
            total_followers += followers
            total_posts += posts
            
            platform_breakdown.append({
                "platform": platform_name,
                "followers": followers,
                "engagement_rate": engagement_rate,
                "posts": posts,
                "likes": platform_data.get('likes', 0),
                "comments": platform_data.get('comments', 0),
                "shares": platform_data.get('shares', 0)
            })
        
        # Calculate average engagement rate
        if platform_breakdown:
            avg_engagement = sum(p['engagement_rate'] for p in platform_breakdown) / len(platform_breakdown)
        else:
            avg_engagement = 0
        
        # Get previous period for comparison
        previous_dates = _calculate_previous_period(dates['start_date'], dates['end_date'])
        previous_query = {
            "user_id": user_id,
            "date": {
                "$gte": previous_dates['start_date'].strftime("%Y-%m-%d"),
                "$lte": previous_dates['end_date'].strftime("%Y-%m-%d")
            }
        }
        
        previous_cursor = db.branding_metrics.find(previous_query).sort("date", -1).limit(1)
        previous_data = await previous_cursor.to_list(length=1)
        
        # Calculate trends
        trends = {}
        if previous_data:
            prev_metrics = previous_data[0]
            prev_platforms = prev_metrics.get('platforms', {})
            
            prev_total_followers = sum(p.get('followers', 0) for p in prev_platforms.values())
            followers_change = _calculate_change(prev_total_followers, total_followers)
            
            trends = {
                "followers_change": followers_change,
                "followers_gained": total_followers - prev_total_followers
            }
        
        # Sort platforms by followers
        platform_breakdown.sort(key=lambda x: x['followers'], reverse=True)
        
        # Get brand sentiment
        sentiment_score = latest_metrics.get('sentiment_score', 0)
        
        response = {
            "user_id": user_id,
            "date_range": {
                "start_date": dates['start_date'].isoformat(),
                "end_date": dates['end_date'].isoformat(),
                "label": date_range
            },
            "summary": {
                "total_followers": total_followers,
                "avg_engagement_rate": round(avg_engagement, 2),
                "total_posts": total_posts,
                "sentiment_score": round(sentiment_score, 2)
            },
            "trends": trends,
            "platform_breakdown": platform_breakdown,
            "last_updated": latest_metrics.get('date')
        }
        
        # Cache for 2 hours
        await redis_service.set(cache_key, response, ttl=7200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching branding overview: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch branding overview: {str(e)}"
        )


@router.get("/platforms")
async def get_connected_platforms(
    user_id: str = Query(...)
) -> Dict[str, Any]:
    """
    Get list of connected social media platforms with their status
    """
    try:
        cache_key = f"branding:platforms:{user_id}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Get latest branding metrics
        latest_metrics = await db.branding_metrics.find_one(
            {"user_id": user_id},
            sort=[("date", -1)]
        )
        
        if not latest_metrics:
            return {
                "user_id": user_id,
                "platforms": [],
                "message": "No social media platforms connected"
            }
        
        platforms = latest_metrics.get('platforms', {})
        
        platforms_list = [
            {
                "platform": platform_name,
                "connected": True,
                "followers": platform_data.get('followers', 0),
                "last_updated": latest_metrics.get('date')
            }
            for platform_name, platform_data in platforms.items()
        ]
        
        response = {
            "user_id": user_id,
            "platforms": platforms_list,
            "total_platforms": len(platforms_list)
        }
        
        # Cache for 1 hour
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching connected platforms: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch connected platforms: {str(e)}"
        )


@router.get("/growth")
async def get_audience_growth(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None, description="Filter by platform"),
    date_range: str = Query("last_90_days")
) -> Dict[str, Any]:
    """
    Get audience growth over time
    
    Returns:
    - Follower growth timeline
    - Net growth
    - Growth rate
    - Daily/weekly growth breakdown
    """
    try:
        cache_key = f"branding:growth:{user_id}:{platform}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": dates['start_date'].strftime("%Y-%m-%d"),
                "$lte": dates['end_date'].strftime("%Y-%m-%d")
            }
        }
        
        # Fetch metrics
        branding_cursor = db.branding_metrics.find(query_filter).sort("date", 1)
        branding_data = await branding_cursor.to_list(length=None)
        
        if not branding_data:
            return {
                "user_id": user_id,
                "message": "No growth data available"
            }
        
        # Build growth timeline
        growth_timeline = []
        
        for metric in branding_data:
            date = metric.get('date')
            platforms = metric.get('platforms', {})
            
            if platform:
                # Single platform
                platform_data = platforms.get(platform, {})
                followers = platform_data.get('followers', 0)
                
                growth_timeline.append({
                    "date": date,
                    "followers": followers,
                    "platform": platform
                })
            else:
                # All platforms combined
                total_followers = sum(p.get('followers', 0) for p in platforms.values())
                
                growth_timeline.append({
                    "date": date,
                    "followers": total_followers,
                    "platform": "all"
                })
        
        # Calculate growth metrics
        if len(growth_timeline) >= 2:
            first_followers = growth_timeline[0]['followers']
            last_followers = growth_timeline[-1]['followers']
            net_growth = last_followers - first_followers
            growth_rate = (net_growth / first_followers * 100) if first_followers > 0 else 0
        else:
            net_growth = 0
            growth_rate = 0
        
        response = {
            "user_id": user_id,
            "platform": platform or "all",
            "date_range": date_range,
            "growth_timeline": growth_timeline,
            "summary": {
                "net_growth": net_growth,
                "growth_rate": round(growth_rate, 2),
                "current_followers": growth_timeline[-1]['followers'] if growth_timeline else 0,
                "starting_followers": growth_timeline[0]['followers'] if growth_timeline else 0
            }
        }
        
        # Cache for 2 hours
        await redis_service.set(cache_key, response, ttl=7200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching audience growth: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch audience growth: {str(e)}"
        )


@router.get("/engagement-summary")
async def get_engagement_summary(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Get engagement summary across all platforms
    
    Returns:
    - Total engagement (likes, comments, shares)
    - Engagement by platform
    - Top performing content types
    - Best posting times
    """
    try:
        cache_key = f"branding:engagement_summary:{user_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": dates['start_date'].strftime("%Y-%m-%d"),
                "$lte": dates['end_date'].strftime("%Y-%m-%d")
            }
        }
        
        # Fetch metrics
        branding_cursor = db.branding_metrics.find(query_filter)
        branding_data = await branding_cursor.to_list(length=None)
        
        if not branding_data:
            return {
                "user_id": user_id,
                "message": "No engagement data available"
            }
        
        # Aggregate engagement
        total_likes = 0
        total_comments = 0
        total_shares = 0
        platform_engagement = {}
        
        for metric in branding_data:
            platforms = metric.get('platforms', {})
            
            for platform_name, platform_data in platforms.items():
                likes = platform_data.get('likes', 0)
                comments = platform_data.get('comments', 0)
                shares = platform_data.get('shares', 0)
                
                total_likes += likes
                total_comments += comments
                total_shares += shares
                
                if platform_name not in platform_engagement:
                    platform_engagement[platform_name] = {
                        "likes": 0,
                        "comments": 0,
                        "shares": 0,
                        "total_engagement": 0
                    }
                
                platform_engagement[platform_name]['likes'] += likes
                platform_engagement[platform_name]['comments'] += comments
                platform_engagement[platform_name]['shares'] += shares
                platform_engagement[platform_name]['total_engagement'] += (likes + comments + shares)
        
        # Format platform engagement
        platform_engagement_list = [
            {
                "platform": platform,
                **data
            }
            for platform, data in platform_engagement.items()
        ]
        
        # Sort by total engagement
        platform_engagement_list.sort(key=lambda x: x['total_engagement'], reverse=True)
        
        total_engagement = total_likes + total_comments + total_shares
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "summary": {
                "total_engagement": total_engagement,
                "total_likes": total_likes,
                "total_comments": total_comments,
                "total_shares": total_shares
            },
            "platform_engagement": platform_engagement_list
        }
        
        # Cache for 2 hours
        await redis_service.set(cache_key, response, ttl=7200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching engagement summary: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch engagement summary: {str(e)}"
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


def _calculate_previous_period(start_date: datetime, end_date: datetime) -> Dict[str, datetime]:
    """Calculate previous period dates for comparison"""
    period_length = (end_date - start_date).days
    previous_end = start_date
    previous_start = previous_end - timedelta(days=period_length)
    
    return {"start_date": previous_start, "end_date": previous_end}


def _calculate_change(previous: float, current: float) -> float:
    """Calculate percentage change"""
    if previous == 0:
        return 0 if current == 0 else 100
    return round(((current - previous) / previous * 100), 2)