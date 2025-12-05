"""
SEO Overview - High-level SEO metrics and performance
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.aggregators.seo_aggregator import SEOAggregator
from app.services.analytics.seo_analytics import SEOAnalytics
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/overview")
async def get_seo_overview(
    user_id: str = Query(..., description="User ID"),
    date_range: str = Query("last_30_days", description="Date range: last_7_days, last_30_days, last_90_days"),
    domain: Optional[str] = Query(None, description="Filter by specific domain")
) -> Dict[str, Any]:
    """
    Get comprehensive SEO overview
    
    Returns:
    - Organic traffic statistics
    - Keyword rankings summary
    - Backlinks count
    - Domain authority/score
    - Top performing pages
    - Search visibility score
    """
    try:
        cache_key = f"seo:overview:{user_id}:{date_range}:{domain}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            logger.info(f"Cache hit for SEO overview: {user_id}")
            return cached_data
        
        logger.info(f"Cache miss for SEO overview: {user_id}")
        
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
        
        if domain:
            query_filter["domain"] = domain
        
        # Fetch SEO metrics
        seo_cursor = db.seo_metrics.find(query_filter).sort("date", -1)
        seo_data = await seo_cursor.to_list(length=None)
        
        if not seo_data:
            return {
                "user_id": user_id,
                "message": "No SEO data available. Please connect Google Search Console.",
                "summary": {},
                "date_range": {
                    "start_date": dates['start_date'].isoformat(),
                    "end_date": dates['end_date'].isoformat()
                }
            }
        
        # Aggregate data
        aggregator = SEOAggregator()
        aggregated = aggregator.aggregate_seo_data(seo_data)
        
        # Calculate analytics
        analytics = SEOAnalytics()
        metrics = analytics.calculate_overview_metrics(aggregated)
        
        # Get previous period for comparison
        previous_period_data = await _get_previous_period_data(
            db, user_id, dates['previous_start_date'], dates['previous_end_date'], domain
        )
        
        previous_metrics = analytics.calculate_overview_metrics(previous_period_data) if previous_period_data else {}
        
        # Calculate trends
        trends = analytics.calculate_trends(metrics, previous_metrics) if previous_metrics else {}
        
        # Get top performing pages
        top_pages = aggregator.get_top_pages(seo_data, limit=10)
        
        # Get top keywords
        top_keywords = aggregator.get_top_keywords(seo_data, limit=10)
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "date_range": {
                "start_date": dates['start_date'].isoformat(),
                "end_date": dates['end_date'].isoformat(),
                "label": date_range
            },
            "summary": {
                "total_organic_traffic": metrics.get('total_organic_traffic', 0),
                "total_impressions": metrics.get('total_impressions', 0),
                "total_clicks": metrics.get('total_clicks', 0),
                "avg_ctr": metrics.get('avg_ctr', 0),
                "avg_position": metrics.get('avg_position', 0),
                "total_keywords": metrics.get('total_keywords', 0),
                "top_10_keywords": metrics.get('top_10_keywords', 0),
                "total_backlinks": metrics.get('total_backlinks', 0),
                "domain_authority": metrics.get('domain_authority', 0)
            },
            "trends": {
                "traffic_change": trends.get('traffic_change', 0),
                "clicks_change": trends.get('clicks_change', 0),
                "impressions_change": trends.get('impressions_change', 0),
                "ctr_change": trends.get('ctr_change', 0),
                "position_change": trends.get('position_change', 0)
            },
            "top_pages": top_pages,
            "top_keywords": top_keywords,
            "last_updated": datetime.utcnow().isoformat()
        }
        
        # Cache for 4 hours
        await redis_service.set(cache_key, response, ttl=14400)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching SEO overview: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch SEO overview: {str(e)}"
        )


@router.get("/overview/domains")
async def get_domains_list(
    user_id: str = Query(...)
) -> Dict[str, Any]:
    """
    Get list of all domains being tracked for this user
    """
    try:
        db = await get_database()
        
        # Get distinct domains
        domains = await db.seo_metrics.distinct("domain", {"user_id": user_id})
        
        # Get latest data for each domain
        domains_data = []
        for domain in domains:
            latest_metric = await db.seo_metrics.find_one(
                {"user_id": user_id, "domain": domain},
                sort=[("date", -1)]
            )
            
            if latest_metric:
                domains_data.append({
                    "domain": domain,
                    "organic_traffic": latest_metric.get('organic_traffic', 0),
                    "total_keywords": len(latest_metric.get('keywords', [])),
                    "avg_position": latest_metric.get('avg_position', 0),
                    "last_updated": latest_metric.get('date')
                })
        
        return {
            "user_id": user_id,
            "domains": domains_data,
            "total_domains": len(domains_data)
        }
        
    except Exception as e:
        logger.error(f"Error fetching domains list: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch domains list: {str(e)}"
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
    
    period_length = (end - start).days
    previous_end = start
    previous_start = previous_end - timedelta(days=period_length)
    
    return {
        "start_date": start,
        "end_date": end,
        "previous_start_date": previous_start,
        "previous_end_date": previous_end
    }


async def _get_previous_period_data(db, user_id: str, start_date: datetime, end_date: datetime, domain: Optional[str]):
    """Get data for previous period"""
    query_filter = {
        "user_id": user_id,
        "date": {
            "$gte": start_date.strftime("%Y-%m-%d"),
            "$lte": end_date.strftime("%Y-%m-%d")
        }
    }
    
    if domain:
        query_filter["domain"] = domain
    
    cursor = db.seo_metrics.find(query_filter)
    data = await cursor.to_list(length=None)
    
    if data:
        aggregator = SEOAggregator()
        return aggregator.aggregate_seo_data(data)
    
    return None