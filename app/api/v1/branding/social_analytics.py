"""
Social Analytics - Platform-specific metrics and content performance
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/social-analytics/platform/{platform}")
async def get_platform_analytics(
    user_id: str = Query(...),
    platform: str = ...,
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Get detailed analytics for a specific platform
    
    Returns:
    - Platform-specific metrics
    - Engagement breakdown
    - Content performance
    - Audience demographics (if available)
    """
    try:
        cache_key = f"branding:platform_analytics:{user_id}:{platform}:{date_range}"
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
                "platform": platform,
                "message": f"No data available for {platform}"
            }
        
        # Extract platform-specific data
        platform_timeline = []
        
        for metric in branding_data:
            platforms = metric.get('platforms', {})
            platform_data = platforms.get(platform)
            
            if platform_data:
                platform_timeline.append({
                    "date": metric.get('date'),
                    "followers": platform_data.get('followers', 0),
                    "engagement_rate": platform_data.get('engagement_rate', 0),
                    "posts": platform_data.get('posts', 0),
                    "likes": platform_data.get('likes', 0),
                    "comments": platform_data.get('comments', 0),
                    "shares": platform_data.get('shares', 0)
                })
        
        if not platform_timeline:
            return {
                "user_id": user_id,
                "platform": platform,
                "message": f"No data available for {platform}"
            }
        
        # Calculate summary
        latest = platform_timeline[-1]
        first = platform_timeline[0]
        
        followers_growth = latest['followers'] - first['followers']
        growth_rate = (followers_growth / first['followers'] * 100) if first['followers'] > 0 else 0
        
        # Average engagement rate
        avg_engagement = sum(p['engagement_rate'] for p in platform_timeline) / len(platform_timeline)
        
        # Total engagement
        total_likes = sum(p['likes'] for p in platform_timeline)
        total_comments = sum(p['comments'] for p in platform_timeline)
        total_shares = sum(p['shares'] for p in platform_timeline)
        
        response = {
            "user_id": user_id,
            "platform": platform,
            "date_range": date_range,
            "summary": {
                "current_followers": latest['followers'],
                "followers_growth": followers_growth,
                "growth_rate": round(growth_rate, 2),
                "avg_engagement_rate": round(avg_engagement, 2),
                "total_posts": sum(p['posts'] for p in platform_timeline),
                "total_likes": total_likes,
                "total_comments": total_comments,
                "total_shares": total_shares
            },
            "timeline": platform_timeline
        }
        
        # Cache for 2 hours
        await redis_service.set(cache_key, response, ttl=7200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching platform analytics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch platform analytics: {str(e)}"
        )


@router.get("/social-analytics/posts")
async def get_posts_analytics(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    date_range: str = Query("last_30_days"),
    sort_by: str = Query("engagement", description="Sort by: engagement, likes, comments, shares, date"),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get analytics for individual posts
    
    Returns:
    - Post performance metrics
    - Top performing posts
    - Content type analysis
    """
    try:
        cache_key = f"branding:posts_analytics:{user_id}:{platform}:{date_range}:{sort_by}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        query_filter = {
            "user_id": user_id,
            "posted_at": {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        }
        
        if platform:
            query_filter["platform"] = platform
        
        # Determine sort field
        sort_field_map = {
            "engagement": "total_engagement",
            "likes": "likes",
            "comments": "comments",
            "shares": "shares",
            "date": "posted_at"
        }
        
        sort_field = sort_field_map.get(sort_by, "total_engagement")
        
        # Fetch posts
        posts_cursor = db.social_posts.find(query_filter).sort(sort_field, -1).limit(limit)
        posts = await posts_cursor.to_list(length=limit)
        
        # Format posts
        formatted_posts = []
        for post in posts:
            likes = post.get('likes', 0)
            comments = post.get('comments', 0)
            shares = post.get('shares', 0)
            total_engagement = likes + comments + shares
            
            formatted_posts.append({
                "id": str(post['_id']),
                "platform": post.get('platform'),
                "post_id": post.get('post_id'),
                "content": post.get('content', '')[:200],  # Truncate for preview
                "post_type": post.get('post_type'),  # image, video, text, link
                "likes": likes,
                "comments": comments,
                "shares": shares,
                "total_engagement": total_engagement,
                "engagement_rate": post.get('engagement_rate', 0),
                "posted_at": post.get('posted_at').isoformat() if post.get('posted_at') else None,
                "url": post.get('url')
            })
        
        # Analyze content types
        content_type_performance = {}
        for post in formatted_posts:
            post_type = post['post_type'] or 'unknown'
            if post_type not in content_type_performance:
                content_type_performance[post_type] = {
                    "count": 0,
                    "total_engagement": 0,
                    "avg_engagement": 0
                }
            
            content_type_performance[post_type]['count'] += 1
            content_type_performance[post_type]['total_engagement'] += post['total_engagement']
        
        # Calculate averages
        for post_type, data in content_type_performance.items():
            data['avg_engagement'] = round(data['total_engagement'] / data['count'], 2) if data['count'] > 0 else 0
        
        response = {
            "user_id": user_id,
            "platform": platform,
            "date_range": date_range,
            "sort_by": sort_by,
            "posts": formatted_posts,
            "total_posts": len(formatted_posts),
            "content_type_performance": content_type_performance
        }
        
        # Cache for 1 hour
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching posts analytics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch posts analytics: {str(e)}"
        )


@router.get("/social-analytics/best-times")
async def get_best_posting_times(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None)
) -> Dict[str, Any]:
    """
    Analyze best times to post based on historical engagement
    
    Returns:
    - Best hours of day
    - Best days of week
    - Recommended posting schedule
    """
    try:
        cache_key = f"branding:best_times:{user_id}:{platform}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Last 90 days for better analysis
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)
        
        query_filter = {
            "user_id": user_id,
            "posted_at": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        
        if platform:
            query_filter["platform"] = platform
        
        # Fetch posts
        posts_cursor = db.social_posts.find(query_filter)
        posts = await posts_cursor.to_list(length=None)
        
        if not posts:
            return {
                "user_id": user_id,
                "platform": platform,
                "message": "Insufficient data for analysis"
            }
        
        # Analyze by hour
        hour_performance = {}
        for post in posts:
            posted_at = post.get('posted_at')
            if posted_at:
                hour = posted_at.hour
                engagement = post.get('likes', 0) + post.get('comments', 0) + post.get('shares', 0)
                
                if hour not in hour_performance:
                    hour_performance[hour] = {
                        "hour": hour,
                        "post_count": 0,
                        "total_engagement": 0,
                        "avg_engagement": 0
                    }
                
                hour_performance[hour]['post_count'] += 1
                hour_performance[hour]['total_engagement'] += engagement
        
        # Calculate averages
        for hour, data in hour_performance.items():
            data['avg_engagement'] = round(data['total_engagement'] / data['post_count'], 2) if data['post_count'] > 0 else 0
        
        # Sort by average engagement
        hour_rankings = sorted(hour_performance.values(), key=lambda x: x['avg_engagement'], reverse=True)
        
        # Analyze by day of week
        day_performance = {}
        day_names = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"}
        
        for post in posts:
            posted_at = post.get('posted_at')
            if posted_at:
                day = posted_at.weekday()
                engagement = post.get('likes', 0) + post.get('comments', 0) + post.get('shares', 0)
                
                if day not in day_performance:
                    day_performance[day] = {
                        "day": day_names[day],
                        "post_count": 0,
                        "total_engagement": 0,
                        "avg_engagement": 0
                    }
                
                day_performance[day]['post_count'] += 1
                day_performance[day]['total_engagement'] += engagement
        
        # Calculate averages
        for day, data in day_performance.items():
            data['avg_engagement'] = round(data['total_engagement'] / data['post_count'], 2) if data['post_count'] > 0 else 0
        
        # Sort by average engagement
        day_rankings = sorted(day_performance.values(), key=lambda x: x['avg_engagement'], reverse=True)
        
        # Generate recommendations
        best_hours = [h['hour'] for h in hour_rankings[:3]]
        best_days = [d['day'] for d in day_rankings[:3]]
        
        response = {
            "user_id": user_id,
            "platform": platform or "all",
            "analysis_period": "last_90_days",
            "hour_rankings": hour_rankings,
            "day_rankings": day_rankings,
            "recommendations": {
                "best_hours": best_hours,
                "best_days": best_days,
                "recommendation": f"Post between {best_hours[0]}:00-{best_hours[0]+1}:00 on {best_days[0]}s for optimal engagement"
            }
        }
        
        # Cache for 24 hours
        await redis_service.set(cache_key, response, ttl=86400)
        
        return response
        
    except Exception as e:
        logger.error(f"Error analyzing best posting times: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to analyze best posting times: {str(e)}"
        )


@router.get("/social-analytics/hashtags")
async def get_hashtag_performance(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Analyze hashtag performance
    
    Returns:
    - Top performing hashtags
    - Hashtag engagement metrics
    - Recommended hashtags
    """
    try:
        cache_key = f"branding:hashtags:{user_id}:{platform}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Last 90 days
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)
        
        query_filter = {
            "user_id": user_id,
            "posted_at": {
                "$gte": start_date,
                "$lte": end_date
            },
            "hashtags": {"$exists": True, "$ne": []}
        }
        
        if platform:
            query_filter["platform"] = platform
        
        # Fetch posts with hashtags
        posts_cursor = db.social_posts.find(query_filter)
        posts = await posts_cursor.to_list(length=None)
        
        if not posts:
            return {
                "user_id": user_id,
                "platform": platform,
                "message": "No hashtag data available"
            }
        
        # Analyze hashtags
        hashtag_performance = {}
        
        for post in posts:
            hashtags = post.get('hashtags', [])
            engagement = post.get('likes', 0) + post.get('comments', 0) + post.get('shares', 0)
            
            for hashtag in hashtags:
                if hashtag not in hashtag_performance:
                    hashtag_performance[hashtag] = {
                        "hashtag": hashtag,
                        "usage_count": 0,
                        "total_engagement": 0,
                        "avg_engagement": 0
                    }
                
                hashtag_performance[hashtag]['usage_count'] += 1
                hashtag_performance[hashtag]['total_engagement'] += engagement
        
        # Calculate averages
        for hashtag, data in hashtag_performance.items():
            data['avg_engagement'] = round(data['total_engagement'] / data['usage_count'], 2) if data['usage_count'] > 0 else 0
        
        # Sort by average engagement
        top_hashtags = sorted(hashtag_performance.values(), key=lambda x: x['avg_engagement'], reverse=True)[:limit]
        
        response = {
            "user_id": user_id,
            "platform": platform or "all",
            "top_hashtags": top_hashtags,
            "total_unique_hashtags": len(hashtag_performance)
        }
        
        # Cache for 12 hours
        await redis_service.set(cache_key, response, ttl=43200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error analyzing hashtag performance: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to analyze hashtag performance: {str(e)}"
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