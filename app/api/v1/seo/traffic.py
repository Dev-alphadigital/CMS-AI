"""
SEO Traffic - Organic traffic analysis
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


@router.get("/traffic/overview")
async def get_traffic_overview(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Get organic traffic overview
    
    Returns:
    - Total organic traffic
    - Traffic by source (Google, Bing, etc.)
    - Traffic trend over time
    - Top landing pages
    - Device breakdown
    """
    try:
        cache_key = f"seo:traffic_overview:{user_id}:{domain}:{date_range}"
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
        elif date_range == "last_90_days":
            start_date = end_date - timedelta(days=90)
        else:
            start_date = end_date - timedelta(days=30)
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        if domain:
            query_filter["domain"] = domain
        
        # Fetch data
        seo_cursor = db.seo_metrics.find(query_filter).sort("date", 1)
        seo_docs = await seo_cursor.to_list(length=None)
        
        if not seo_docs:
            return {
                "user_id": user_id,
                "message": "No traffic data available"
            }
        
        # Aggregate traffic data
        total_traffic = sum(doc.get('organic_traffic', 0) for doc in seo_docs)
        total_clicks = sum(
            sum(kw.get('clicks', 0) for kw in doc.get('keywords', []))
            for doc in seo_docs
        )
        total_impressions = sum(
            sum(kw.get('impressions', 0) for kw in doc.get('keywords', []))
            for doc in seo_docs
        )
        
        # Time series
        time_series = [
            {
                "date": doc.get('date'),
                "traffic": doc.get('organic_traffic', 0),
                "clicks": sum(kw.get('clicks', 0) for kw in doc.get('keywords', [])),
                "impressions": sum(kw.get('impressions', 0) for kw in doc.get('keywords', []))
            }
            for doc in seo_docs
        ]
        
        # Get top landing pages (aggregate by URL)
        url_traffic = {}
        for doc in seo_docs:
            for kw in doc.get('keywords', []):
                url = kw.get('url', 'unknown')
                clicks = kw.get('clicks', 0)
                if url in url_traffic:
                    url_traffic[url] += clicks
                else:
                    url_traffic[url] = clicks
        
        top_pages = sorted(
            [{"url": url, "clicks": clicks} for url, clicks in url_traffic.items()],
            key=lambda x: x['clicks'],
            reverse=True
        )[:10]
        
        # Calculate average CTR
        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "label": date_range
            },
            "summary": {
                "total_traffic": total_traffic,
                "total_clicks": total_clicks,
                "total_impressions": total_impressions,
                "avg_ctr": round(avg_ctr, 2)
            },
            "time_series": time_series,
            "top_landing_pages": top_pages
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching traffic overview: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch traffic overview: {str(e)}"
        )


@router.get("/traffic/sources")
async def get_traffic_sources(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Get traffic breakdown by source (search engines, referrals, etc.)
    
    Note: This requires Google Analytics integration
    """
    try:
        cache_key = f"seo:traffic_sources:{user_id}:{domain}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        # TODO: Implement Google Analytics integration
        # For now, return placeholder
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "message": "Traffic sources require Google Analytics integration",
            "sources": []
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching traffic sources: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch traffic sources: {str(e)}"
        )


@router.get("/traffic/pages")
async def get_top_pages(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    date_range: str = Query("last_30_days"),
    limit: int = Query(20, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get top performing pages by organic traffic
    """
    try:
        cache_key = f"seo:traffic_pages:{user_id}:{domain}:{date_range}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        end_date = datetime.utcnow()
        if date_range == "last_30_days":
            start_date = end_date - timedelta(days=30)
        else:
            start_date = end_date - timedelta(days=30)
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        if domain:
            query_filter["domain"] = domain
        
        seo_cursor = db.seo_metrics.find(query_filter)
        seo_docs = await seo_cursor.to_list(length=None)
        
        # Aggregate by URL
        page_metrics = {}
        
        for doc in seo_docs:
            for kw in doc.get('keywords', []):
                url = kw.get('url', 'unknown')
                clicks = kw.get('clicks', 0)
                impressions = kw.get('impressions', 0)
                
                if url not in page_metrics:
                    page_metrics[url] = {
                        "clicks": 0,
                        "impressions": 0,
                        "keywords": set()
                    }
                
                page_metrics[url]['clicks'] += clicks
                page_metrics[url]['impressions'] += impressions
                page_metrics[url]['keywords'].add(kw.get('keyword'))
        
        # Format and sort
        pages = []
        for url, metrics in page_metrics.items():
            ctr = (metrics['clicks'] / metrics['impressions'] * 100) if metrics['impressions'] > 0 else 0
            pages.append({
                "url": url,
                "clicks": metrics['clicks'],
                "impressions": metrics['impressions'],
                "ctr": round(ctr, 2),
                "keyword_count": len(metrics['keywords'])
            })
        
        pages.sort(key=lambda x: x['clicks'], reverse=True)
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "pages": pages[:limit],
            "total_pages": len(pages)
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching top pages: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch top pages: {str(e)}"
        )