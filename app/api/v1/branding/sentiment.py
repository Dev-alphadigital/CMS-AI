"""
Brand Sentiment - Sentiment analysis and brand health monitoring
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.ai.sentiment_analyzer import SentimentAnalyzer
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/sentiment/overview")
async def get_sentiment_overview(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Get overall brand sentiment analysis
    
    Returns:
    - Current sentiment score (-1 to 1)
    - Sentiment breakdown (positive, neutral, negative)
    - Sentiment trend over time
    - Platform-specific sentiment
    """
    try:
        cache_key = f"branding:sentiment_overview:{user_id}:{date_range}"
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
        
        # Fetch branding metrics with sentiment
        branding_cursor = db.branding_metrics.find(query_filter).sort("date", 1)
        branding_data = await branding_cursor.to_list(length=None)
        
        if not branding_data:
            return {
                "user_id": user_id,
                "message": "No sentiment data available"
            }
        
        # Get latest sentiment score
        latest_sentiment = branding_data[-1].get('sentiment_score', 0)
        
        # Calculate sentiment breakdown from mentions/comments
        mentions_query = {
            "user_id": user_id,
            "created_at": {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        }
        
        mentions_cursor = db.brand_mentions.find(mentions_query)
        mentions = await mentions_cursor.to_list(length=None)
        
        # Count sentiment categories
        positive_count = sum(1 for m in mentions if m.get('sentiment', 0) > 0.1)
        negative_count = sum(1 for m in mentions if m.get('sentiment', 0) < -0.1)
        neutral_count = len(mentions) - positive_count - negative_count
        
        total_mentions = len(mentions)
        
        sentiment_breakdown = {
            "positive": {
                "count": positive_count,
                "percentage": round((positive_count / total_mentions * 100), 2) if total_mentions > 0 else 0
            },
            "neutral": {
                "count": neutral_count,
                "percentage": round((neutral_count / total_mentions * 100), 2) if total_mentions > 0 else 0
            },
            "negative": {
                "count": negative_count,
                "percentage": round((negative_count / total_mentions * 100), 2) if total_mentions > 0 else 0
            }
        }
        
        # Sentiment trend over time
        sentiment_timeline = [
            {
                "date": metric.get('date'),
                "sentiment_score": round(metric.get('sentiment_score', 0), 3)
            }
            for metric in branding_data
        ]
        
        # Platform-specific sentiment
        platform_sentiment = {}
        for mention in mentions:
            platform = mention.get('platform')
            if platform:
                if platform not in platform_sentiment:
                    platform_sentiment[platform] = {
                        "total": 0,
                        "positive": 0,
                        "negative": 0,
                        "neutral": 0,
                        "avg_sentiment": 0
                    }
                
                platform_sentiment[platform]['total'] += 1
                sentiment = mention.get('sentiment', 0)
                
                if sentiment > 0.1:
                    platform_sentiment[platform]['positive'] += 1
                elif sentiment < -0.1:
                    platform_sentiment[platform]['negative'] += 1
                else:
                    platform_sentiment[platform]['neutral'] += 1
                
                platform_sentiment[platform]['avg_sentiment'] += sentiment
        
        # Calculate averages
        for platform, data in platform_sentiment.items():
            if data['total'] > 0:
                data['avg_sentiment'] = round(data['avg_sentiment'] / data['total'], 3)
        
        # Determine sentiment health
        if latest_sentiment > 0.3:
            health = "excellent"
        elif latest_sentiment > 0.1:
            health = "good"
        elif latest_sentiment > -0.1:
            health = "neutral"
        elif latest_sentiment > -0.3:
            health = "concerning"
        else:
            health = "critical"
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "current_sentiment": {
                "score": round(latest_sentiment, 3),
                "health": health
            },
            "sentiment_breakdown": sentiment_breakdown,
            "total_mentions": total_mentions,
            "sentiment_timeline": sentiment_timeline,
            "platform_sentiment": platform_sentiment
        }
        
        # Cache for 2 hours
        await redis_service.set(cache_key, response, ttl=7200)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching sentiment overview: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch sentiment overview: {str(e)}"
        )


@router.get("/sentiment/mentions")
async def get_brand_mentions(
    user_id: str = Query(...),
    sentiment_filter: Optional[str] = Query(None, description="Filter: positive, negative, neutral"),
    platform: Optional[str] = Query(None),
    date_range: str = Query("last_7_days"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get brand mentions with sentiment analysis
    
    Returns:
    - Mentions across platforms
    - Sentiment scores
    - User/source information
    - Content preview
    """
    try:
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        # Build query filter
        query_filter = {
            "user_id": user_id,
            "created_at": {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        }
        
        if platform:
            query_filter["platform"] = platform
        
        # Filter by sentiment
        if sentiment_filter:
            if sentiment_filter == "positive":
                query_filter["sentiment"] = {"$gt": 0.1}
            elif sentiment_filter == "negative":
                query_filter["sentiment"] = {"$lt": -0.1}
            elif sentiment_filter == "neutral":
                query_filter["sentiment"] = {"$gte": -0.1, "$lte": 0.1}
        
        # Count total
        total_count = await db.brand_mentions.count_documents(query_filter)
        
        # Calculate pagination
        skip = (page - 1) * limit
        
        # Fetch mentions
        mentions_cursor = db.brand_mentions.find(query_filter).sort("created_at", -1).skip(skip).limit(limit)
        mentions = await mentions_cursor.to_list(length=limit)
        
        # Format mentions
        formatted_mentions = []
        for mention in mentions:
            sentiment_score = mention.get('sentiment', 0)
            
            # Determine sentiment label
            if sentiment_score > 0.1:
                sentiment_label = "positive"
            elif sentiment_score < -0.1:
                sentiment_label = "negative"
            else:
                sentiment_label = "neutral"
            
            formatted_mentions.append({
                "id": str(mention['_id']),
                "platform": mention.get('platform'),
                "author": mention.get('author'),
                "content": mention.get('content'),
                "sentiment_score": round(sentiment_score, 3),
                "sentiment_label": sentiment_label,
                "url": mention.get('url'),
                "created_at": mention.get('created_at').isoformat() if mention.get('created_at') else None,
                "engagement": {
                    "likes": mention.get('likes', 0),
                    "comments": mention.get('comments', 0),
                    "shares": mention.get('shares', 0)
                }
            })
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        
        response = {
            "user_id": user_id,
            "filters": {
                "sentiment": sentiment_filter,
                "platform": platform,
                "date_range": date_range
            },
            "mentions": formatted_mentions,
            "pagination": {
                "page": page,
                "limit": limit,
                "total_mentions": total_count,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching brand mentions: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch brand mentions: {str(e)}"
        )


@router.get("/sentiment/keywords")
async def get_sentiment_keywords(
    user_id: str = Query(...),
    sentiment_type: str = Query("all", description="Type: all, positive, negative"),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get most common keywords/phrases in mentions by sentiment
    
    Returns:
    - Top keywords
    - Frequency
    - Associated sentiment
    """
    try:
        cache_key = f"branding:sentiment_keywords:{user_id}:{sentiment_type}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Last 30 days
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)
        
        query_filter = {
            "user_id": user_id,
            "created_at": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        
        # Filter by sentiment type
        if sentiment_type == "positive":
            query_filter["sentiment"] = {"$gt": 0.1}
        elif sentiment_type == "negative":
            query_filter["sentiment"] = {"$lt": -0.1}
        
        # Fetch mentions
        mentions_cursor = db.brand_mentions.find(query_filter)
        mentions = await mentions_cursor.to_list(length=None)
        
        # TODO: Implement actual keyword extraction using NLP
        # For now, return placeholder structure
        
        keywords = []
        
        # Simple keyword extraction (this should be improved with proper NLP)
        keyword_counts = {}
        for mention in mentions:
            content = mention.get('content', '').lower()
            words = content.split()
            
            for word in words:
                if len(word) > 4:  # Filter short words
                    if word not in keyword_counts:
                        keyword_counts[word] = {
                            "keyword": word,
                            "count": 0,
                            "sentiment_sum": 0
                        }
                    keyword_counts[word]['count'] += 1
                    keyword_counts[word]['sentiment_sum'] += mention.get('sentiment', 0)
        
        # Calculate average sentiment per keyword
        for word, data in keyword_counts.items():
            data['avg_sentiment'] = round(data['sentiment_sum'] / data['count'], 3) if data['count'] > 0 else 0
            keywords.append(data)
        
        # Sort by count
        keywords.sort(key=lambda x: x['count'], reverse=True)
        
        response = {
            "user_id": user_id,
            "sentiment_type": sentiment_type,
            "keywords": keywords[:limit],
            "total_unique_keywords": len(keywords),
            "note": "Keyword extraction can be enhanced with NLP libraries"
        }
        
        # Cache for 6 hours
        await redis_service.set(cache_key, response, ttl=21600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching sentiment keywords: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch sentiment keywords: {str(e)}"
        )


@router.get("/sentiment/alerts")
async def get_sentiment_alerts(
    user_id: str = Query(...)
) -> Dict[str, Any]:
    """
    Get sentiment alerts and warnings
    
    Returns:
    - Sudden negative sentiment spikes
    - Viral negative mentions
    - Potential reputation issues
    """
    try:
        cache_key = f"branding:sentiment_alerts:{user_id}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Last 7 days
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=7)
        
        # Get recent mentions
        mentions_cursor = db.brand_mentions.find({
            "user_id": user_id,
            "created_at": {"$gte": start_date, "$lte": end_date}
        }).sort("created_at", -1)
        
        mentions = await mentions_cursor.to_list(length=None)
        
        alerts = []
        
        # Check for negative mentions with high engagement
        for mention in mentions:
            sentiment = mention.get('sentiment', 0)
            engagement = mention.get('likes', 0) + mention.get('comments', 0) + mention.get('shares', 0)
            
            if sentiment < -0.3 and engagement > 100:
                alerts.append({
                    "type": "viral_negative",
                    "severity": "high",
                    "message": f"High-engagement negative mention on {mention.get('platform')}",
                    "mention_id": str(mention['_id']),
                    "sentiment_score": round(sentiment, 3),
                    "engagement": engagement,
                    "created_at": mention.get('created_at').isoformat() if mention.get('created_at') else None
                })
        
        # Check for sentiment drops
        recent_metrics = await db.branding_metrics.find({
            "user_id": user_id
        }).sort("date", -1).limit(7).to_list(length=7)
        
        if len(recent_metrics) >= 2:
            latest_sentiment = recent_metrics[0].get('sentiment_score', 0)
            previous_sentiment = recent_metrics[1].get('sentiment_score', 0)
            
            sentiment_change = latest_sentiment - previous_sentiment
            
            if sentiment_change < -0.2:
                alerts.append({
                    "type": "sentiment_drop",
                    "severity": "medium",
                    "message": f"Significant sentiment drop detected: {round(sentiment_change, 3)}",
                    "current_sentiment": round(latest_sentiment, 3),
                    "previous_sentiment": round(previous_sentiment, 3)
                })
        
        # Sort by severity
        severity_order = {"high": 0, "medium": 1, "low": 2}
        alerts.sort(key=lambda x: severity_order.get(x['severity'], 3))
        
        response = {
            "user_id": user_id,
            "alerts": alerts,
            "total_alerts": len(alerts),
            "last_checked": end_date.isoformat()
        }
        
        # Cache for 30 minutes
        await redis_service.set(cache_key, response, ttl=1800)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching sentiment alerts: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch sentiment alerts: {str(e)}"
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