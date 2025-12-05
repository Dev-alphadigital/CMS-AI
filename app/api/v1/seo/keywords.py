"""
SEO Keywords - Keyword tracking and management
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


@router.get("/keywords")
async def get_keywords(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    position_range: Optional[str] = Query(None, description="Filter: top_3, top_10, top_20, top_50, top_100"),
    sort_by: str = Query("volume", description="Sort by: volume, position, clicks, impressions"),
    order: str = Query("desc"),
    limit: int = Query(50, ge=1, le=200)
) -> Dict[str, Any]:
    """
    Get list of tracked keywords with their rankings and metrics
    
    Returns:
    - Keyword
    - Current position
    - Search volume
    - Clicks, impressions, CTR
    - Position change (trend)
    """
    try:
        cache_key = f"seo:keywords:{user_id}:{domain}:{position_range}:{sort_by}:{order}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Get latest SEO data
        query_filter = {"user_id": user_id}
        if domain:
            query_filter["domain"] = domain
        
        # Get most recent date
        latest_doc = await db.seo_metrics.find_one(
            query_filter,
            sort=[("date", -1)]
        )
        
        if not latest_doc:
            return {
                "user_id": user_id,
                "keywords": [],
                "total": 0,
                "message": "No keyword data available"
            }
        
        latest_date = latest_doc['date']
        query_filter["date"] = latest_date
        
        # Get all documents for this date (might be multiple if multiple domains)
        seo_cursor = db.seo_metrics.find(query_filter)
        seo_docs = await seo_cursor.to_list(length=None)
        
        # Extract and flatten keywords
        all_keywords = []
        for doc in seo_docs:
            keywords_list = doc.get('keywords', [])
            for kw in keywords_list:
                keyword_data = {
                    "keyword": kw.get('keyword'),
                    "position": kw.get('position'),
                    "volume": kw.get('volume', 0),
                    "clicks": kw.get('clicks', 0),
                    "impressions": kw.get('impressions', 0),
                    "ctr": kw.get('ctr', 0),
                    "domain": doc.get('domain'),
                    "url": kw.get('url'),
                    "date": latest_date
                }
                
                # Apply position filter
                if position_range:
                    position = kw.get('position', 999)
                    if position_range == "top_3" and position <= 3:
                        all_keywords.append(keyword_data)
                    elif position_range == "top_10" and position <= 10:
                        all_keywords.append(keyword_data)
                    elif position_range == "top_20" and position <= 20:
                        all_keywords.append(keyword_data)
                    elif position_range == "top_50" and position <= 50:
                        all_keywords.append(keyword_data)
                    elif position_range == "top_100" and position <= 100:
                        all_keywords.append(keyword_data)
                else:
                    all_keywords.append(keyword_data)
        
        # Sort keywords
        reverse = (order == "desc")
        if sort_by == "volume":
            all_keywords.sort(key=lambda x: x.get('volume', 0), reverse=reverse)
        elif sort_by == "position":
            all_keywords.sort(key=lambda x: x.get('position', 999), reverse=not reverse)
        elif sort_by == "clicks":
            all_keywords.sort(key=lambda x: x.get('clicks', 0), reverse=reverse)
        elif sort_by == "impressions":
            all_keywords.sort(key=lambda x: x.get('impressions', 0), reverse=reverse)
        
        # Get position changes (compare with 7 days ago)
        seven_days_ago = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        previous_query = {**query_filter, "date": seven_days_ago}
        previous_cursor = db.seo_metrics.find(previous_query)
        previous_docs = await previous_cursor.to_list(length=None)
        
        # Build previous position map
        previous_positions = {}
        for doc in previous_docs:
            for kw in doc.get('keywords', []):
                previous_positions[kw.get('keyword')] = kw.get('position')
        
        # Add position change to keywords
        for kw in all_keywords:
            previous_pos = previous_positions.get(kw['keyword'])
            if previous_pos:
                kw['position_change'] = previous_pos - kw['position']  # Positive = improvement
            else:
                kw['position_change'] = None
        
        # Limit results
        limited_keywords = all_keywords[:limit]
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "filters": {
                "position_range": position_range,
                "sort_by": sort_by,
                "order": order
            },
            "keywords": limited_keywords,
            "total": len(all_keywords),
            "showing": len(limited_keywords),
            "date": latest_date
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching keywords: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch keywords: {str(e)}"
        )


@router.get("/keywords/{keyword}")
async def get_keyword_details(
    user_id: str = Query(...),
    keyword: str = ...,
    domain: Optional[str] = Query(None),
    date_range: str = Query("last_90_days")
) -> Dict[str, Any]:
    """
    Get detailed historical data for a specific keyword
    
    Returns:
    - Position history (time series)
    - Clicks/impressions over time
    - Ranking URLs
    - Competitor analysis
    """
    try:
        cache_key = f"seo:keyword_details:{user_id}:{keyword}:{domain}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        end_date = datetime.utcnow()
        if date_range == "last_30_days":
            start_date = end_date - timedelta(days=30)
        elif date_range == "last_90_days":
            start_date = end_date - timedelta(days=90)
        else:
            start_date = end_date - timedelta(days=90)
        
        # Build query
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            },
            "keywords.keyword": keyword
        }
        
        if domain:
            query_filter["domain"] = domain
        
        # Fetch historical data
        seo_cursor = db.seo_metrics.find(query_filter).sort("date", 1)
        seo_docs = await seo_cursor.to_list(length=None)
        
        if not seo_docs:
            raise HTTPException(
                status_code=404,
                detail="Keyword not found"
            )
        
        # Extract keyword data over time
        time_series = []
        for doc in seo_docs:
            for kw in doc.get('keywords', []):
                if kw.get('keyword') == keyword:
                    time_series.append({
                        "date": doc.get('date'),
                        "position": kw.get('position'),
                        "clicks": kw.get('clicks', 0),
                        "impressions": kw.get('impressions', 0),
                        "ctr": kw.get('ctr', 0),
                        "url": kw.get('url')
                    })
                    break
        
        # Calculate statistics
        positions = [entry['position'] for entry in time_series if entry['position']]
        avg_position = sum(positions) / len(positions) if positions else 0
        best_position = min(positions) if positions else None
        worst_position = max(positions) if positions else None
        
        total_clicks = sum(entry['clicks'] for entry in time_series)
        total_impressions = sum(entry['impressions'] for entry in time_series)
        
        # Get latest data
        latest = time_series[-1] if time_series else {}
        
        response = {
            "user_id": user_id,
            "keyword": keyword,
            "domain": domain,
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "current_data": {
                "position": latest.get('position'),
                "url": latest.get('url'),
                "clicks": latest.get('clicks'),
                "impressions": latest.get('impressions'),
                "ctr": latest.get('ctr')
            },
            "statistics": {
                "avg_position": round(avg_position, 2),
                "best_position": best_position,
                "worst_position": worst_position,
                "total_clicks": total_clicks,
                "total_impressions": total_impressions
            },
            "time_series": time_series
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching keyword details: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch keyword details: {str(e)}"
        )


@router.get("/keywords/opportunities")
async def get_keyword_opportunities(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get keyword opportunities (keywords in positions 11-20 that could be improved to page 1)
    
    Returns:
    - Keywords ranking 11-20
    - High volume keywords with potential
    - Quick win opportunities
    """
    try:
        cache_key = f"seo:keyword_opportunities:{user_id}:{domain}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        query_filter = {"user_id": user_id}
        if domain:
            query_filter["domain"] = domain
        
        # Get latest data
        latest_doc = await db.seo_metrics.find_one(query_filter, sort=[("date", -1)])
        
        if not latest_doc:
            return {
                "user_id": user_id,
                "opportunities": [],
                "total": 0
            }
        
        # Find keywords in position 11-20 with high volume
        opportunities = []
        for kw in latest_doc.get('keywords', []):
            position = kw.get('position', 999)
            volume = kw.get('volume', 0)
            
            if 11 <= position <= 20 and volume >= 100:  # High volume threshold
                opportunities.append({
                    "keyword": kw.get('keyword'),
                    "position": position,
                    "volume": volume,
                    "clicks": kw.get('clicks', 0),
                    "impressions": kw.get('impressions', 0),
                    "url": kw.get('url'),
                    "potential_clicks": int(volume * 0.15),  # Estimate if moved to position 3-5
                    "opportunity_score": (volume * (21 - position))  # Higher volume + closer to page 1 = better
                })
        
        # Sort by opportunity score
        opportunities.sort(key=lambda x: x['opportunity_score'], reverse=True)
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "opportunities": opportunities[:limit],
            "total": len(opportunities),
            "description": "Keywords ranking 11-20 with high search volume that could be improved to page 1"
        }
        
        await redis_service.set(cache_key, response, ttl=7200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching keyword opportunities: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch keyword opportunities: {str(e)}"
        )