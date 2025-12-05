"""
Ads Campaigns - Campaign-level details and management
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


@router.get("/campaigns")
async def get_campaigns(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="Filter by status: active, paused, ended"),
    sort_by: str = Query("spend", description="Sort by: spend, clicks, conversions, roas"),
    order: str = Query("desc", description="Order: asc, desc"),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get list of campaigns with filters and sorting
    """
    try:
        db = await get_database()
        
        # Build query
        query_filter = {"user_id": user_id}
        if platform:
            query_filter["platform"] = platform
        if status:
            query_filter["status"] = status
        
        # Get latest metrics for each campaign (last 7 days aggregated)
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=7)
        
        pipeline = [
            {"$match": {
                **query_filter,
                "metrics.date": {
                    "$gte": start_date.strftime("%Y-%m-%d"),
                    "$lte": end_date.strftime("%Y-%m-%d")
                }
            }},
            {"$group": {
                "_id": {
                    "campaign_id": "$campaign_id",
                    "campaign_name": "$campaign_name",
                    "platform": "$platform"
                },
                "total_spend": {"$sum": "$metrics.spend"},
                "total_impressions": {"$sum": "$metrics.impressions"},
                "total_clicks": {"$sum": "$metrics.clicks"},
                "total_conversions": {"$sum": "$metrics.conversions"},
                "avg_ctr": {"$avg": "$metrics.ctr"},
                "avg_cpc": {"$avg": "$metrics.cpc"},
                "avg_roas": {"$avg": "$metrics.roas"}
            }},
            {"$sort": {f"total_{sort_by}": -1 if order == "desc" else 1}},
            {"$limit": limit}
        ]
        
        campaigns_cursor = db.ad_campaigns.aggregate(pipeline)
        campaigns = await campaigns_cursor.to_list(length=None)
        
        # Format response
        formatted_campaigns = []
        for campaign in campaigns:
            formatted_campaigns.append({
                "campaign_id": campaign['_id']['campaign_id'],
                "campaign_name": campaign['_id']['campaign_name'],
                "platform": campaign['_id']['platform'],
                "metrics": {
                    "spend": round(campaign['total_spend'], 2),
                    "impressions": campaign['total_impressions'],
                    "clicks": campaign['total_clicks'],
                    "conversions": campaign['total_conversions'],
                    "ctr": round(campaign['avg_ctr'], 2),
                    "cpc": round(campaign['avg_cpc'], 2),
                    "roas": round(campaign['avg_roas'], 2)
                }
            })
        
        return {
            "user_id": user_id,
            "filters": {
                "platform": platform,
                "status": status,
                "sort_by": sort_by,
                "order": order
            },
            "campaigns": formatted_campaigns,
            "total_campaigns": len(formatted_campaigns),
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error fetching campaigns: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch campaigns: {str(e)}"
        )


@router.get("/campaigns/{campaign_id}")
async def get_campaign_details(
    user_id: str = Query(...),
    campaign_id: str = ...,
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Get detailed information about a specific campaign
    
    Returns:
    - Campaign overview
    - Performance over time
    - Ad sets/ad groups breakdown
    - Audience insights
    """
    try:
        cache_key = f"ads:campaign:{user_id}:{campaign_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        end_date = datetime.utcnow()
        if date_range == "last_7_days":
            start_date = end_date - timedelta(days=7)
        elif date_range == "last_30_days":
            start_date = end_date - timedelta(days=30)
        else:
            start_date = end_date - timedelta(days=30)
        
        # Get campaign data
        query_filter = {
            "user_id": user_id,
            "campaign_id": campaign_id,
            "metrics.date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        campaigns_cursor = db.ad_campaigns.find(query_filter).sort("metrics.date", 1)
        campaign_data = await campaigns_cursor.to_list(length=None)
        
        if not campaign_data:
            raise HTTPException(
                status_code=404,
                detail="Campaign not found"
            )
        
        # Aggregate metrics
        total_spend = sum(c['metrics']['spend'] for c in campaign_data)
        total_impressions = sum(c['metrics']['impressions'] for c in campaign_data)
        total_clicks = sum(c['metrics']['clicks'] for c in campaign_data)
        total_conversions = sum(c['metrics']['conversions'] for c in campaign_data)
        
        # Calculate averages
        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        avg_cpc = (total_spend / total_clicks) if total_clicks > 0 else 0
        conversion_rate = (total_conversions / total_clicks * 100) if total_clicks > 0 else 0
        
        # Time series data
        time_series = [
            {
                "date": c['metrics']['date'],
                "spend": c['metrics']['spend'],
                "clicks": c['metrics']['clicks'],
                "conversions": c['metrics']['conversions'],
                "roas": c['metrics'].get('roas', 0)
            }
            for c in campaign_data
        ]
        
        response = {
            "campaign_id": campaign_id,
            "campaign_name": campaign_data[0]['campaign_name'],
            "platform": campaign_data[0]['platform'],
            "status": campaign_data[0].get('status', 'active'),
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "summary": {
                "total_spend": round(total_spend, 2),
                "total_impressions": total_impressions,
                "total_clicks": total_clicks,
                "total_conversions": total_conversions,
                "avg_ctr": round(avg_ctr, 2),
                "avg_cpc": round(avg_cpc, 2),
                "conversion_rate": round(conversion_rate, 2)
            },
            "time_series": time_series
        }
        
        await redis_service.set(cache_key, response, ttl=1800)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching campaign details: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch campaign details: {str(e)}"
        )


@router.get("/campaigns/top-performers")
async def get_top_performing_campaigns(
    user_id: str = Query(...),
    metric: str = Query("roas", description="Metric to rank by: roas, conversions, ctr"),
    platform: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=50)
) -> Dict[str, Any]:
    """
    Get top performing campaigns based on specific metric
    """
    try:
        db = await get_database()
        
        # Last 30 days
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)
        
        query_filter = {
            "user_id": user_id,
            "metrics.date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        if platform:
            query_filter["platform"] = platform
        
        # Map metric to aggregation field
        metric_field_map = {
            "roas": "avg_roas",
            "conversions": "total_conversions",
            "ctr": "avg_ctr",
            "spend": "total_spend"
        }
        
        sort_field = metric_field_map.get(metric, "avg_roas")
        
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": {
                    "campaign_id": "$campaign_id",
                    "campaign_name": "$campaign_name",
                    "platform": "$platform"
                },
                "total_spend": {"$sum": "$metrics.spend"},
                "total_conversions": {"$sum": "$metrics.conversions"},
                "avg_roas": {"$avg": "$metrics.roas"},
                "avg_ctr": {"$avg": "$metrics.ctr"}
            }},
            {"$sort": {sort_field: -1}},
            {"$limit": limit}
        ]
        
        campaigns_cursor = db.ad_campaigns.aggregate(pipeline)
        top_campaigns = await campaigns_cursor.to_list(length=None)
        
        formatted_campaigns = [
            {
                "campaign_id": c['_id']['campaign_id'],
                "campaign_name": c['_id']['campaign_name'],
                "platform": c['_id']['platform'],
                "metric_value": round(c[sort_field], 2),
                "total_spend": round(c['total_spend'], 2),
                "total_conversions": c['total_conversions']
            }
            for c in top_campaigns
        ]
        
        return {
            "user_id": user_id,
            "metric": metric,
            "platform_filter": platform,
            "top_campaigns": formatted_campaigns,
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error fetching top performing campaigns: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch top performing campaigns: {str(e)}"
        )