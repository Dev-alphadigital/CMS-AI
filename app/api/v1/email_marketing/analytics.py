"""
Email Marketing Analytics - Performance analysis and insights
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.analytics.email_analytics import EmailAnalytics
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/analytics/overview")
async def get_analytics_overview(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days", description="Date range: last_7_days, last_30_days, last_90_days, all_time")
) -> Dict[str, Any]:
    """
    Get email marketing performance overview
    
    Returns:
    - Total campaigns sent
    - Aggregate metrics (opens, clicks, bounces)
    - Average performance rates
    - Trends compared to previous period
    """
    try:
        cache_key = f"email:analytics_overview:{user_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        # Build query
        query_filter = {
            "user_id": user_id,
            "status": "sent"
        }
        
        if date_range != "all_time":
            query_filter["sent_at"] = {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        
        # Aggregate metrics
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": None,
                "total_campaigns": {"$sum": 1},
                "total_sent": {"$sum": "$sent"},
                "total_opened": {"$sum": "$opened"},
                "total_clicked": {"$sum": "$clicked"},
                "total_bounced": {"$sum": "$bounced"},
                "total_unsubscribed": {"$sum": "$unsubscribed"},
                "avg_open_rate": {"$avg": "$open_rate"},
                "avg_click_rate": {"$avg": "$click_rate"},
                "avg_bounce_rate": {"$avg": "$bounce_rate"}
            }}
        ]
        
        metrics_cursor = db.email_campaigns.aggregate(pipeline)
        metrics_result = await metrics_cursor.to_list(length=1)
        
        if not metrics_result:
            return {
                "user_id": user_id,
                "date_range": date_range,
                "message": "No email campaigns found for this period"
            }
        
        metrics = metrics_result[0]
        
        # Calculate previous period for comparison
        previous_dates = _calculate_previous_period(dates['start_date'], dates['end_date'])
        previous_query = {
            "user_id": user_id,
            "status": "sent",
            "sent_at": {
                "$gte": previous_dates['start_date'],
                "$lte": previous_dates['end_date']
            }
        }
        
        previous_cursor = db.email_campaigns.aggregate([
            {"$match": previous_query},
            {"$group": {
                "_id": None,
                "total_sent": {"$sum": "$sent"},
                "total_opened": {"$sum": "$opened"},
                "total_clicked": {"$sum": "$clicked"}
            }}
        ])
        
        previous_result = await previous_cursor.to_list(length=1)
        
        # Calculate trends
        trends = {}
        if previous_result:
            prev = previous_result[0]
            
            sent_change = _calculate_change(prev.get('total_sent', 0), metrics['total_sent'])
            opened_change = _calculate_change(prev.get('total_opened', 0), metrics['total_opened'])
            clicked_change = _calculate_change(prev.get('total_clicked', 0), metrics['total_clicked'])
            
            trends = {
                "sent_change": sent_change,
                "opened_change": opened_change,
                "clicked_change": clicked_change
            }
        
        response = {
            "user_id": user_id,
            "date_range": {
                "label": date_range,
                "start_date": dates['start_date'].isoformat() if date_range != "all_time" else None,
                "end_date": dates['end_date'].isoformat() if date_range != "all_time" else None
            },
            "summary": {
                "total_campaigns": metrics['total_campaigns'],
                "total_sent": metrics['total_sent'],
                "total_opened": metrics['total_opened'],
                "total_clicked": metrics['total_clicked'],
                "total_bounced": metrics['total_bounced'],
                "total_unsubscribed": metrics['total_unsubscribed'],
                "avg_open_rate": round(metrics['avg_open_rate'], 2),
                "avg_click_rate": round(metrics['avg_click_rate'], 2),
                "avg_bounce_rate": round(metrics['avg_bounce_rate'], 2)
            },
            "trends": trends
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching analytics overview: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch analytics overview: {str(e)}"
        )


@router.get("/analytics/performance-over-time")
async def get_performance_over_time(
    user_id: str = Query(...),
    date_range: str = Query("last_90_days"),
    group_by: str = Query("week", description="Group by: day, week, month")
) -> Dict[str, Any]:
    """
    Get email performance trends over time
    
    Returns:
    - Time series data for opens, clicks, sends
    - Performance trends
    """
    try:
        cache_key = f"email:performance_over_time:{user_id}:{date_range}:{group_by}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        query_filter = {
            "user_id": user_id,
            "status": "sent",
            "sent_at": {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        }
        
        # Determine date grouping format
        if group_by == "day":
            date_format = "%Y-%m-%d"
        elif group_by == "week":
            date_format = "%Y-W%V"
        else:  # month
            date_format = "%Y-%m"
        
        # Aggregate by time period
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": {
                    "$dateToString": {
                        "format": date_format,
                        "date": "$sent_at"
                    }
                },
                "campaigns_sent": {"$sum": 1},
                "total_sent": {"$sum": "$sent"},
                "total_opened": {"$sum": "$opened"},
                "total_clicked": {"$sum": "$clicked"},
                "avg_open_rate": {"$avg": "$open_rate"},
                "avg_click_rate": {"$avg": "$click_rate"}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        time_series_cursor = db.email_campaigns.aggregate(pipeline)
        time_series_data = await time_series_cursor.to_list(length=None)
        
        formatted_data = [
            {
                "period": item['_id'],
                "campaigns_sent": item['campaigns_sent'],
                "total_sent": item['total_sent'],
                "total_opened": item['total_opened'],
                "total_clicked": item['total_clicked'],
                "avg_open_rate": round(item['avg_open_rate'], 2),
                "avg_click_rate": round(item['avg_click_rate'], 2)
            }
            for item in time_series_data
        ]
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "group_by": group_by,
            "time_series": formatted_data
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching performance over time: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch performance over time: {str(e)}"
        )


@router.get("/analytics/engagement")
async def get_engagement_analytics(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Get engagement analytics
    
    Returns:
    - Best performing subject lines
    - Best send times
    - Device engagement breakdown
    - Email client breakdown
    """
    try:
        cache_key = f"email:engagement:{user_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        query_filter = {
            "user_id": user_id,
            "status": "sent",
            "sent_at": {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        }
        
        # Get campaigns
        campaigns_cursor = db.email_campaigns.find(query_filter)
        campaigns = await campaigns_cursor.to_list(length=None)
        
        if not campaigns:
            return {
                "user_id": user_id,
                "message": "No campaigns found for engagement analysis"
            }
        
        # Analyze best subject lines
        subject_performance = sorted(
            [
                {
                    "subject": c.get('subject'),
                    "open_rate": c.get('open_rate', 0),
                    "click_rate": c.get('click_rate', 0),
                    "sent": c.get('sent', 0)
                }
                for c in campaigns if c.get('sent', 0) > 100  # Filter for campaigns with >100 sends
            ],
            key=lambda x: x['open_rate'],
            reverse=True
        )[:10]
        
        # Analyze best send times
        send_time_performance = {}
        for c in campaigns:
            if c.get('sent_at'):
                hour = c['sent_at'].hour
                if hour not in send_time_performance:
                    send_time_performance[hour] = {
                        "hour": hour,
                        "campaigns": 0,
                        "avg_open_rate": 0,
                        "avg_click_rate": 0
                    }
                send_time_performance[hour]['campaigns'] += 1
                send_time_performance[hour]['avg_open_rate'] += c.get('open_rate', 0)
                send_time_performance[hour]['avg_click_rate'] += c.get('click_rate', 0)
        
        # Calculate averages
        for hour, data in send_time_performance.items():
            count = data['campaigns']
            data['avg_open_rate'] = round(data['avg_open_rate'] / count, 2)
            data['avg_click_rate'] = round(data['avg_click_rate'] / count, 2)
        
        send_times = sorted(send_time_performance.values(), key=lambda x: x['avg_open_rate'], reverse=True)
        
        # Aggregate device breakdown
        device_totals = {}
        for c in campaigns:
            device_data = c.get('device_breakdown', {})
            for device, count in device_data.items():
                device_totals[device] = device_totals.get(device, 0) + count
        
        # Aggregate email client breakdown
        client_totals = {}
        for c in campaigns:
            client_data = c.get('email_client_breakdown', {})
            for client, count in client_data.items():
                client_totals[client] = client_totals.get(client, 0) + count
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "top_subject_lines": subject_performance,
            "best_send_times": send_times,
            "device_breakdown": device_totals,
            "email_client_breakdown": client_totals
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching engagement analytics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch engagement analytics: {str(e)}"
        )


@router.get("/analytics/list-growth")
async def get_list_growth(
    user_id: str = Query(...),
    date_range: str = Query("last_90_days")
) -> Dict[str, Any]:
    """
    Get email list growth analytics
    
    Returns:
    - New subscribers
    - Unsubscribes
    - Net growth
    - Growth trend
    """
    try:
        cache_key = f"email:list_growth:{user_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        # TODO: This requires subscriber list tracking
        # For now, return placeholder
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "message": "List growth tracking requires subscriber database implementation",
            "growth_data": []
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching list growth: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch list growth: {str(e)}"
        )


@router.get("/analytics/benchmarks")
async def get_industry_benchmarks(
    user_id: str = Query(...),
    industry: Optional[str] = Query(None)
) -> Dict[str, Any]:
    """
    Get industry benchmark comparisons
    
    Returns:
    - User's performance vs industry averages
    - Percentile ranking
    """
    try:
        # Industry average benchmarks (approximate)
        industry_benchmarks = {
            "general": {
                "avg_open_rate": 21.5,
                "avg_click_rate": 2.6,
                "avg_bounce_rate": 0.7,
                "avg_unsubscribe_rate": 0.25
            },
            "ecommerce": {
                "avg_open_rate": 15.7,
                "avg_click_rate": 2.3,
                "avg_bounce_rate": 0.6,
                "avg_unsubscribe_rate": 0.31
            },
            "saas": {
                "avg_open_rate": 21.0,
                "avg_click_rate": 2.8,
                "avg_bounce_rate": 0.8,
                "avg_unsubscribe_rate": 0.28
            },
            "media": {
                "avg_open_rate": 22.3,
                "avg_click_rate": 3.9,
                "avg_bounce_rate": 0.5,
                "avg_unsubscribe_rate": 0.19
            }
        }
        
        db = await get_database()
        
        # Get user's overall performance
        pipeline = [
            {"$match": {"user_id": user_id, "status": "sent"}},
            {"$group": {
                "_id": None,
                "avg_open_rate": {"$avg": "$open_rate"},
                "avg_click_rate": {"$avg": "$click_rate"},
                "avg_bounce_rate": {"$avg": "$bounce_rate"}
            }}
        ]
        
        user_stats_cursor = db.email_campaigns.aggregate(pipeline)
        user_stats_result = await user_stats_cursor.to_list(length=1)
        
        if not user_stats_result:
            return {
                "user_id": user_id,
                "message": "No campaign data available for benchmarking"
            }
        
        user_stats = user_stats_result[0]
        
        # Get benchmark for industry
        benchmark = industry_benchmarks.get(industry or "general", industry_benchmarks["general"])
        
        # Calculate comparisons
        open_rate_diff = user_stats['avg_open_rate'] - benchmark['avg_open_rate']
        click_rate_diff = user_stats['avg_click_rate'] - benchmark['avg_click_rate']
        bounce_rate_diff = user_stats['avg_bounce_rate'] - benchmark['avg_bounce_rate']
        
        response = {
            "user_id": user_id,
            "industry": industry or "general",
            "your_performance": {
                "avg_open_rate": round(user_stats['avg_open_rate'], 2),
                "avg_click_rate": round(user_stats['avg_click_rate'], 2),
                "avg_bounce_rate": round(user_stats['avg_bounce_rate'], 2)
            },
            "industry_benchmark": benchmark,
            "comparison": {
                "open_rate_diff": round(open_rate_diff, 2),
                "click_rate_diff": round(click_rate_diff, 2),
                "bounce_rate_diff": round(bounce_rate_diff, 2),
                "performance_summary": "above_average" if open_rate_diff > 0 and click_rate_diff > 0 else "below_average"
            }
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching industry benchmarks: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch industry benchmarks: {str(e)}"
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