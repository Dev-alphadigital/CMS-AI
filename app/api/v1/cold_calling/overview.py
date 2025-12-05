"""
Cold Calling Overview - High-level call metrics and dashboard
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.analytics.call_analytics import CallAnalytics
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/overview")
async def get_overview(
    user_id: str = Query(...),
    date_range: str = Query("today", description="Date range: today, yesterday, last_7_days, last_30_days"),
    agent_id: Optional[str] = Query(None, description="Filter by specific agent")
) -> Dict[str, Any]:
    """
    Get cold calling overview dashboard
    
    Returns:
    - Total calls made
    - Call outcomes breakdown
    - Average call duration
    - Success rate
    - Agent performance summary
    - Hourly call distribution
    """
    try:
        cache_key = f"calls:overview:{user_id}:{date_range}:{agent_id}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            logger.info(f"Cache hit for cold calling overview: {user_id}")
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        # Build query filter
        query_filter = {
            "user_id": user_id,
            "called_at": {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        }
        
        if agent_id:
            query_filter["agent_id"] = agent_id
        
        # Aggregate call statistics
        pipeline = [
            {"$match": query_filter},
            {"$facet": {
                "total_stats": [
                    {"$group": {
                        "_id": None,
                        "total_calls": {"$sum": 1},
                        "total_duration": {"$sum": "$duration"},
                        "avg_duration": {"$avg": "$duration"}
                    }}
                ],
                "outcome_breakdown": [
                    {"$group": {
                        "_id": "$outcome",
                        "count": {"$sum": 1}
                    }}
                ],
                "agent_performance": [
                    {"$group": {
                        "_id": "$agent_name",
                        "calls_made": {"$sum": 1},
                        "total_duration": {"$sum": "$duration"}
                    }},
                    {"$sort": {"calls_made": -1}}
                ],
                "hourly_distribution": [
                    {"$group": {
                        "_id": {"$hour": "$called_at"},
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"_id": 1}}
                ],
                "daily_trend": [
                    {"$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$called_at"
                            }
                        },
                        "calls": {"$sum": 1}
                    }},
                    {"$sort": {"_id": 1}}
                ]
            }}
        ]
        
        stats_cursor = db.cold_calls.aggregate(pipeline)
        stats_result = await stats_cursor.to_list(length=1)
        
        if not stats_result or not stats_result[0]['total_stats']:
            return {
                "user_id": user_id,
                "date_range": date_range,
                "message": "No call data available for this period",
                "summary": {
                    "total_calls": 0,
                    "total_duration": 0,
                    "avg_duration": 0
                }
            }
        
        stats = stats_result[0]
        total_stats = stats['total_stats'][0]
        
        # Format outcome breakdown
        outcome_breakdown = {
            item['_id']: item['count'] 
            for item in stats['outcome_breakdown']
        }
        
        # Calculate success rate (interested + callback as success)
        total_calls = total_stats['total_calls']
        successful_calls = outcome_breakdown.get('interested', 0) + outcome_breakdown.get('callback', 0)
        success_rate = (successful_calls / total_calls * 100) if total_calls > 0 else 0
        
        # Format agent performance
        agent_performance = [
            {
                "agent_name": agent['_id'],
                "calls_made": agent['calls_made'],
                "total_duration_minutes": round(agent['total_duration'] / 60, 2),
                "avg_duration_seconds": round(agent['total_duration'] / agent['calls_made'], 2) if agent['calls_made'] > 0 else 0
            }
            for agent in stats['agent_performance'][:10]  # Top 10 agents
        ]
        
        # Format hourly distribution
        hourly_distribution = [
            {"hour": item['_id'], "calls": item['count']}
            for item in stats['hourly_distribution']
        ]
        
        # Format daily trend
        daily_trend = [
            {"date": item['_id'], "calls": item['calls']}
            for item in stats['daily_trend']
        ]
        
        # Get previous period for comparison
        previous_dates = _calculate_previous_period(dates['start_date'], dates['end_date'])
        previous_query = {
            "user_id": user_id,
            "called_at": {
                "$gte": previous_dates['start_date'],
                "$lte": previous_dates['end_date']
            }
        }
        
        if agent_id:
            previous_query["agent_id"] = agent_id
        
        previous_count = await db.cold_calls.count_documents(previous_query)
        
        # Calculate trends
        calls_change = _calculate_change(previous_count, total_calls)
        
        response = {
            "user_id": user_id,
            "date_range": {
                "label": date_range,
                "start_date": dates['start_date'].isoformat(),
                "end_date": dates['end_date'].isoformat()
            },
            "summary": {
                "total_calls": total_calls,
                "total_duration_minutes": round(total_stats['total_duration'] / 60, 2),
                "avg_duration_seconds": round(total_stats['avg_duration'], 2),
                "success_rate": round(success_rate, 2)
            },
            "outcome_breakdown": outcome_breakdown,
            "trends": {
                "calls_change": calls_change
            },
            "top_agents": agent_performance,
            "hourly_distribution": hourly_distribution,
            "daily_trend": daily_trend
        }
        
        # Cache for 5 minutes
        await redis_service.set(cache_key, response, ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching cold calling overview: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch cold calling overview: {str(e)}"
        )


@router.get("/stats/realtime")
async def get_realtime_stats(
    user_id: str = Query(...)
) -> Dict[str, Any]:
    """
    Get real-time call statistics (today's activity)
    
    Returns:
    - Calls made today
    - Active calls
    - Calls in last hour
    - Current agent activity
    """
    try:
        db = await get_database()
        
        # Today's date range
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        now = datetime.utcnow()
        
        # Calls made today
        today_query = {
            "user_id": user_id,
            "called_at": {"$gte": today_start}
        }
        
        calls_today = await db.cold_calls.count_documents(today_query)
        
        # Calls in last hour
        last_hour = now - timedelta(hours=1)
        last_hour_query = {
            "user_id": user_id,
            "called_at": {"$gte": last_hour}
        }
        
        calls_last_hour = await db.cold_calls.count_documents(last_hour_query)
        
        # Get latest calls (last 5)
        latest_cursor = db.cold_calls.find(today_query).sort("called_at", -1).limit(5)
        latest_calls = await latest_cursor.to_list(length=5)
        
        formatted_latest = [
            {
                "agent_name": call.get('agent_name'),
                "customer_phone": call.get('customer_phone'),
                "outcome": call.get('outcome'),
                "duration": call.get('duration'),
                "called_at": call.get('called_at').isoformat() if call.get('called_at') else None
            }
            for call in latest_calls
        ]
        
        # Active agents (made calls in last 30 minutes)
        active_threshold = now - timedelta(minutes=30)
        active_agents_pipeline = [
            {
                "$match": {
                    "user_id": user_id,
                    "called_at": {"$gte": active_threshold}
                }
            },
            {
                "$group": {
                    "_id": "$agent_name",
                    "last_call": {"$max": "$called_at"},
                    "calls_in_period": {"$sum": 1}
                }
            }
        ]
        
        active_agents_cursor = db.cold_calls.aggregate(active_agents_pipeline)
        active_agents_data = await active_agents_cursor.to_list(length=None)
        
        active_agents = [
            {
                "agent_name": agent['_id'],
                "last_call": agent['last_call'].isoformat(),
                "calls_in_last_30min": agent['calls_in_period']
            }
            for agent in active_agents_data
        ]
        
        response = {
            "user_id": user_id,
            "timestamp": now.isoformat(),
            "calls_today": calls_today,
            "calls_last_hour": calls_last_hour,
            "active_agents": len(active_agents),
            "active_agents_list": active_agents,
            "latest_calls": formatted_latest
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching realtime stats: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch realtime stats: {str(e)}"
        )


@router.get("/stats/agents")
async def get_agents_summary(
    user_id: str = Query(...),
    date_range: str = Query("today")
) -> Dict[str, Any]:
    """
    Get summary of all agents' performance
    
    Returns:
    - List of agents with their metrics
    - Calls made
    - Success rate
    - Average duration
    """
    try:
        cache_key = f"calls:agents_summary:{user_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        query_filter = {
            "user_id": user_id,
            "called_at": {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        }
        
        # Aggregate by agent
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": {
                    "agent_id": "$agent_id",
                    "agent_name": "$agent_name"
                },
                "total_calls": {"$sum": 1},
                "total_duration": {"$sum": "$duration"},
                "interested_count": {
                    "$sum": {"$cond": [{"$eq": ["$outcome", "interested"]}, 1, 0]}
                },
                "callback_count": {
                    "$sum": {"$cond": [{"$eq": ["$outcome", "callback"]}, 1, 0]}
                },
                "not_interested_count": {
                    "$sum": {"$cond": [{"$eq": ["$outcome", "not_interested"]}, 1, 0]}
                },
                "voicemail_count": {
                    "$sum": {"$cond": [{"$eq": ["$outcome", "voicemail"]}, 1, 0]}
                },
                "no_answer_count": {
                    "$sum": {"$cond": [{"$eq": ["$outcome", "no_answer"]}, 1, 0]}
                }
            }},
            {"$sort": {"total_calls": -1}}
        ]
        
        agents_cursor = db.cold_calls.aggregate(pipeline)
        agents_data = await agents_cursor.to_list(length=None)
        
        # Format agent summaries
        agents_summary = []
        for agent in agents_data:
            total_calls = agent['total_calls']
            successful = agent['interested_count'] + agent['callback_count']
            success_rate = (successful / total_calls * 100) if total_calls > 0 else 0
            
            agents_summary.append({
                "agent_id": agent['_id']['agent_id'],
                "agent_name": agent['_id']['agent_name'],
                "total_calls": total_calls,
                "total_duration_minutes": round(agent['total_duration'] / 60, 2),
                "avg_duration_seconds": round(agent['total_duration'] / total_calls, 2) if total_calls > 0 else 0,
                "success_rate": round(success_rate, 2),
                "outcome_breakdown": {
                    "interested": agent['interested_count'],
                    "callback": agent['callback_count'],
                    "not_interested": agent['not_interested_count'],
                    "voicemail": agent['voicemail_count'],
                    "no_answer": agent['no_answer_count']
                }
            })
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "agents": agents_summary,
            "total_agents": len(agents_summary)
        }
        
        # Cache for 5 minutes
        await redis_service.set(cache_key, response, ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching agents summary: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch agents summary: {str(e)}"
        )


def _calculate_date_range(date_range: str) -> Dict[str, datetime]:
    """Calculate start and end dates"""
    end = datetime.utcnow()
    
    if date_range == "today":
        start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_range == "yesterday":
        start = (end - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_range == "last_7_days":
        start = end - timedelta(days=7)
    elif date_range == "last_30_days":
        start = end - timedelta(days=30)
    else:
        start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    
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