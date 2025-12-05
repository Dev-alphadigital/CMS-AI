"""
SEO Rankings - Position tracking and analysis
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


@router.get("/rankings/distribution")
async def get_ranking_distribution(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None)
) -> Dict[str, Any]:
    """
    Get distribution of keyword rankings across position ranges
    
    Returns:
    - Count of keywords in positions: 1-3, 4-10, 11-20, 21-50, 51-100, 100+
    - Percentage breakdown
    - Comparison with previous period
    """
    try:
        cache_key = f"seo:ranking_distribution:{user_id}:{domain}"
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
                "distribution": {},
                "message": "No ranking data available"
            }
        
        # Calculate distribution
        distribution = {
            "top_3": 0,
            "top_10": 0,
            "top_20": 0,
            "top_50": 0,
            "top_100": 0,
            "beyond_100": 0
        }
        
        keywords = latest_doc.get('keywords', [])
        total_keywords = len(keywords)
        
        for kw in keywords:
            position = kw.get('position', 999)
            
            if position <= 3:
                distribution["top_3"] += 1
            elif position <= 10:
                distribution["top_10"] += 1
            elif position <= 20:
                distribution["top_20"] += 1
            elif position <= 50:
                distribution["top_50"] += 1
            elif position <= 100:
                distribution["top_100"] += 1
            else:
                distribution["beyond_100"] += 1
        
        # Calculate percentages
        distribution_percentage = {}
        for key, count in distribution.items():
            distribution_percentage[key] = {
                "count": count,
                "percentage": round((count / total_keywords * 100), 2) if total_keywords > 0 else 0
            }
        
        # Get previous period (30 days ago)
        previous_date = (datetime.strptime(latest_doc['date'], "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        previous_query = {**query_filter, "date": previous_date}
        previous_doc = await db.seo_metrics.find_one(previous_query)
        
        previous_distribution = None
        if previous_doc:
            prev_dist = {
                "top_3": 0,
                "top_10": 0,
                "top_20": 0,
                "top_50": 0,
                "top_100": 0,
                "beyond_100": 0
            }
            
            for kw in previous_doc.get('keywords', []):
                position = kw.get('position', 999)
                if position <= 3:
                    prev_dist["top_3"] += 1
                elif position <= 10:
                    prev_dist["top_10"] += 1
                elif position <= 20:
                    prev_dist["top_20"] += 1
                elif position <= 50:
                    prev_dist["top_50"] += 1
                elif position <= 100:
                    prev_dist["top_100"] += 1
                else:
                    prev_dist["beyond_100"] += 1
            
            previous_distribution = prev_dist
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "date": latest_doc['date'],
            "total_keywords": total_keywords,
            "distribution": distribution_percentage,
            "previous_distribution": previous_distribution,
            "changes": _calculate_distribution_changes(distribution, previous_distribution) if previous_distribution else None
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching ranking distribution: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch ranking distribution: {str(e)}"
        )


@router.get("/rankings/movement")
async def get_ranking_movement(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    days: int = Query(7, description="Compare with N days ago")
) -> Dict[str, Any]:
    """
    Get keywords with biggest ranking movements (up/down)
    
    Returns:
    - Top gainers (biggest position improvements)
    - Top losers (biggest position drops)
    - New rankings (keywords that just entered top 100)
    - Lost rankings (keywords that dropped out of top 100)
    """
    try:
        cache_key = f"seo:ranking_movement:{user_id}:{domain}:{days}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        query_filter = {"user_id": user_id}
        if domain:
            query_filter["domain"] = domain
        
        # Get current data
        current_doc = await db.seo_metrics.find_one(query_filter, sort=[("date", -1)])
        
        if not current_doc:
            return {
                "user_id": user_id,
                "message": "No ranking data available"
            }
        
        current_date = current_doc['date']
        
        # Get previous data
        comparison_date = (datetime.strptime(current_date, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
        previous_query = {**query_filter, "date": comparison_date}
        previous_doc = await db.seo_metrics.find_one(previous_query)
        
        if not previous_doc:
            return {
                "user_id": user_id,
                "message": f"No data available for {days} days ago for comparison"
            }
        
        # Build keyword position maps
        current_positions = {kw['keyword']: kw['position'] for kw in current_doc.get('keywords', [])}
        previous_positions = {kw['keyword']: kw['position'] for kw in previous_doc.get('keywords', [])}
        
        # Calculate movements
        gainers = []
        losers = []
        new_rankings = []
        lost_rankings = []
        
        for keyword, current_pos in current_positions.items():
            if keyword in previous_positions:
                previous_pos = previous_positions[keyword]
                movement = previous_pos - current_pos  # Positive = improvement
                
                if movement > 0:
                    gainers.append({
                        "keyword": keyword,
                        "current_position": current_pos,
                        "previous_position": previous_pos,
                        "movement": movement
                    })
                elif movement < 0:
                    losers.append({
                        "keyword": keyword,
                        "current_position": current_pos,
                        "previous_position": previous_pos,
                        "movement": movement
                    })
            else:
                # New keyword in rankings
                if current_pos <= 100:
                    new_rankings.append({
                        "keyword": keyword,
                        "current_position": current_pos
                    })
        
        # Find lost rankings
        for keyword, previous_pos in previous_positions.items():
            if keyword not in current_positions and previous_pos <= 100:
                lost_rankings.append({
                    "keyword": keyword,
                    "previous_position": previous_pos
                })
        
        # Sort
        gainers.sort(key=lambda x: x['movement'], reverse=True)
        losers.sort(key=lambda x: x['movement'])
        new_rankings.sort(key=lambda x: x['current_position'])
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "comparison_period": f"{days} days",
            "current_date": current_date,
            "comparison_date": comparison_date,
            "top_gainers": gainers[:20],
            "top_losers": losers[:20],
            "new_rankings": new_rankings[:20],
            "lost_rankings": lost_rankings[:20],
            "summary": {
                "total_gainers": len(gainers),
                "total_losers": len(losers),
                "new_rankings_count": len(new_rankings),
                "lost_rankings_count": len(lost_rankings)
            }
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching ranking movement: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch ranking movement: {str(e)}"
        )


@router.get("/rankings/visibility")
async def get_search_visibility(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    date_range: str = Query("last_90_days")
) -> Dict[str, Any]:
    """
    Calculate search visibility score over time
    
    Search Visibility = Sum of (Search Volume × CTR for Position) for all keywords
    
    Returns:
    - Visibility score over time
    - Visibility by keyword category
    - Trending up/down
    """
    try:
        cache_key = f"seo:search_visibility:{user_id}:{domain}:{date_range}"
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
                "message": "No data available for visibility calculation"
            }
        
        # Expected CTR by position (industry averages)
        ctr_by_position = {
            1: 0.316, 2: 0.158, 3: 0.107, 4: 0.076, 5: 0.058,
            6: 0.047, 7: 0.039, 8: 0.033, 9: 0.029, 10: 0.025,
        }
        
        # Calculate visibility over time
        visibility_time_series = []
        
        for doc in seo_docs:
            total_visibility = 0
            keywords = doc.get('keywords', [])
            
            for kw in keywords:
                position = kw.get('position', 999)
                volume = kw.get('volume', 0)
                
                if position <= 100:
                    # Get expected CTR for this position
                    if position <= 10:
                        expected_ctr = ctr_by_position.get(position, 0.025)
                    elif position <= 20:
                        expected_ctr = 0.015
                    elif position <= 50:
                        expected_ctr = 0.005
                    else:
                        expected_ctr = 0.001
                    
                    visibility_contribution = volume * expected_ctr
                    total_visibility += visibility_contribution
            
            visibility_time_series.append({
                "date": doc.get('date'),
                "visibility_score": round(total_visibility, 2)
            })
        
        # Calculate trend
        if len(visibility_time_series) >= 2:
            first_score = visibility_time_series[0]['visibility_score']
            last_score = visibility_time_series[-1]['visibility_score']
            trend_percentage = ((last_score - first_score) / first_score * 100) if first_score > 0 else 0
        else:
            trend_percentage = 0
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "current_visibility_score": visibility_time_series[-1]['visibility_score'] if visibility_time_series else 0,
            "trend_percentage": round(trend_percentage, 2),
            "time_series": visibility_time_series
        }
        
        await redis_service.set(cache_key, response, ttl=7200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error calculating search visibility: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate search visibility: {str(e)}"
        )


def _calculate_distribution_changes(current: Dict, previous: Dict) -> Dict:
    """Calculate changes between current and previous distribution"""
    changes = {}
    for key in current.keys():
        changes[key] = current[key] - previous.get(key, 0)
    return changes