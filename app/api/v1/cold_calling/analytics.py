"""
Cold Calling Analytics - Performance analysis and insights
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


@router.get("/analytics/performance")
async def get_performance_analytics(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days", description="Date range: last_7_days, last_30_days, last_90_days"),
    agent_id: Optional[str] = Query(None),
    group_by: str = Query("day", description="Group by: day, week, month")
) -> Dict[str, Any]:
    """
    Get detailed performance analytics with time series
    
    Returns:
    - Calls over time
    - Success rate trends
    - Outcome distribution over time
    - Peak performance hours/days
    """
    try:
        cache_key = f"calls:analytics_performance:{user_id}:{date_range}:{agent_id}:{group_by}"
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
        
        if agent_id:
            query_filter["agent_id"] = agent_id
        
        # Determine date grouping format
        if group_by == "day":
            date_format = "%Y-%m-%d"
        elif group_by == "week":
            date_format = "%Y-W%V"
        else:  # month
            date_format = "%Y-%m"
        
        # Time series aggregation
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": {
                    "$dateToString": {
                        "format": date_format,
                        "date": "$called_at"
                    }
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
            {"$sort": {"_id": 1}}
        ]
        
        time_series_cursor = db.cold_calls.aggregate(pipeline)
        time_series_data = await time_series_cursor.to_list(length=None)
        
        # Format time series
        formatted_time_series = []
        for item in time_series_data:
            total_calls = item['total_calls']
            successful_calls = item['interested_count'] + item['callback_count']
            success_rate = (successful_calls / total_calls * 100) if total_calls > 0 else 0
            
            formatted_time_series.append({
                "period": item['_id'],
                "total_calls": total_calls,
                "total_duration_minutes": round(item['total_duration'] / 60, 2),
                "avg_duration_seconds": round(item['total_duration'] / total_calls, 2) if total_calls > 0 else 0,
                "success_rate": round(success_rate, 2),
                "outcome_breakdown": {
                    "interested": item['interested_count'],
                    "callback": item['callback_count'],
                    "not_interested": item['not_interested_count'],
                    "voicemail": item['voicemail_count'],
                    "no_answer": item['no_answer_count']
                }
            })
        
        # Get day of week performance
        day_of_week_pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": {"$dayOfWeek": "$called_at"},
                "total_calls": {"$sum": 1},
                "successful_calls": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$outcome", ["interested", "callback"]]},
                            1,
                            0
                        ]
                    }
                }
            }},
            {"$sort": {"_id": 1}}
        ]
        
        day_of_week_cursor = db.cold_calls.aggregate(day_of_week_pipeline)
        day_of_week_data = await day_of_week_cursor.to_list(length=None)
        
        # Map day numbers to names
        day_names = {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday", 5: "Thursday", 6: "Friday", 7: "Saturday"}
        
        day_of_week_performance = [
            {
                "day": day_names[item['_id']],
                "total_calls": item['total_calls'],
                "success_rate": round((item['successful_calls'] / item['total_calls'] * 100), 2) if item['total_calls'] > 0 else 0
            }
            for item in day_of_week_data
        ]
        
        # Get hour of day performance
        hour_of_day_pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": {"$hour": "$called_at"},
                "total_calls": {"$sum": 1},
                "successful_calls": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$outcome", ["interested", "callback"]]},
                            1,
                            0
                        ]
                    }
                }
            }},
            {"$sort": {"_id": 1}}
        ]
        
        hour_of_day_cursor = db.cold_calls.aggregate(hour_of_day_pipeline)
        hour_of_day_data = await hour_of_day_cursor.to_list(length=None)
        
        hour_of_day_performance = [
            {
                "hour": item['_id'],
                "total_calls": item['total_calls'],
                "success_rate": round((item['successful_calls'] / item['total_calls'] * 100), 2) if item['total_calls'] > 0 else 0
            }
            for item in hour_of_day_data
        ]
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "agent_id": agent_id,
            "group_by": group_by,
            "time_series": formatted_time_series,
            "day_of_week_performance": day_of_week_performance,
            "hour_of_day_performance": hour_of_day_performance
        }
        
        # Cache for 1 hour
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching performance analytics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch performance analytics: {str(e)}"
        )


@router.get("/analytics/agents/comparison")
async def get_agents_comparison(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days")
) -> Dict[str, Any]:
    """
    Compare performance across all agents
    
    Returns:
    - Side-by-side agent comparison
    - Top performers
    - Areas for improvement
    """
    try:
        cache_key = f"calls:agents_comparison:{user_id}:{date_range}"
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
        
        # Aggregate by agent with detailed metrics
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
            }}
        ]
        
        agents_cursor = db.cold_calls.aggregate(pipeline)
        agents_data = await agents_cursor.to_list(length=None)
        
        # Calculate metrics for each agent
        agents_comparison = []
        for agent in agents_data:
            total_calls = agent['total_calls']
            successful_calls = agent['interested_count'] + agent['callback_count']
            success_rate = (successful_calls / total_calls * 100) if total_calls > 0 else 0
            
            # Calculate contact rate (not voicemail/no answer)
            contacted = total_calls - agent['voicemail_count'] - agent['no_answer_count']
            contact_rate = (contacted / total_calls * 100) if total_calls > 0 else 0
            
            # Calculate conversion rate (interested from contacted)
            conversion_rate = (agent['interested_count'] / contacted * 100) if contacted > 0 else 0
            
            agents_comparison.append({
                "agent_id": agent['_id']['agent_id'],
                "agent_name": agent['_id']['agent_name'],
                "metrics": {
                    "total_calls": total_calls,
                    "total_duration_minutes": round(agent['total_duration'] / 60, 2),
                    "avg_call_duration_seconds": round(agent['total_duration'] / total_calls, 2) if total_calls > 0 else 0,
                    "success_rate": round(success_rate, 2),
                    "contact_rate": round(contact_rate, 2),
                    "conversion_rate": round(conversion_rate, 2)
                },
                "outcomes": {
                    "interested": agent['interested_count'],
                    "callback": agent['callback_count'],
                    "not_interested": agent['not_interested_count'],
                    "voicemail": agent['voicemail_count'],
                    "no_answer": agent['no_answer_count']
                }
            })
        
        # Sort by success rate
        agents_comparison.sort(key=lambda x: x['metrics']['success_rate'], reverse=True)
        
        # Identify top performers
        if agents_comparison:
            top_performer = {
                "by_success_rate": agents_comparison[0]['agent_name'],
                "by_total_calls": max(agents_comparison, key=lambda x: x['metrics']['total_calls'])['agent_name'],
                "by_conversion_rate": max(agents_comparison, key=lambda x: x['metrics']['conversion_rate'])['agent_name']
            }
        else:
            top_performer = None
        
        # Calculate team averages
        if agents_comparison:
            team_avg_success_rate = sum(a['metrics']['success_rate'] for a in agents_comparison) / len(agents_comparison)
            team_avg_contact_rate = sum(a['metrics']['contact_rate'] for a in agents_comparison) / len(agents_comparison)
            team_avg_calls = sum(a['metrics']['total_calls'] for a in agents_comparison) / len(agents_comparison)
        else:
            team_avg_success_rate = 0
            team_avg_contact_rate = 0
            team_avg_calls = 0
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "agents_comparison": agents_comparison,
            "total_agents": len(agents_comparison),
            "top_performers": top_performer,
            "team_averages": {
                "avg_success_rate": round(team_avg_success_rate, 2),
                "avg_contact_rate": round(team_avg_contact_rate, 2),
                "avg_calls_per_agent": round(team_avg_calls, 2)
            }
        }
        
        # Cache for 1 hour
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching agents comparison: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch agents comparison: {str(e)}"
        )


@router.get("/analytics/conversion-funnel")
async def get_conversion_funnel(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days"),
    agent_id: Optional[str] = Query(None)
) -> Dict[str, Any]:
    """
    Get conversion funnel analysis
    
    Returns:
    - Total calls attempted
    - Successfully connected
    - Interested responses
    - Scheduled callbacks
    - Drop-off rates at each stage
    """
    try:
        cache_key = f"calls:conversion_funnel:{user_id}:{date_range}:{agent_id}"
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
        
        if agent_id:
            query_filter["agent_id"] = agent_id
        
        # Get outcome counts
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": "$outcome",
                "count": {"$sum": 1}
            }}
        ]
        
        outcome_cursor = db.cold_calls.aggregate(pipeline)
        outcome_data = await outcome_cursor.to_list(length=None)
        
        outcome_counts = {item['_id']: item['count'] for item in outcome_data}
        
        # Calculate funnel stages
        total_attempts = sum(outcome_counts.values())
        
        # Stage 1: Total calls attempted
        stage_1_total = total_attempts
        
        # Stage 2: Successfully contacted (not voicemail/no answer)
        stage_2_contacted = (
            outcome_counts.get('interested', 0) +
            outcome_counts.get('callback', 0) +
            outcome_counts.get('not_interested', 0)
        )
        
        # Stage 3: Positive response (interested + callback)
        stage_3_positive = (
            outcome_counts.get('interested', 0) +
            outcome_counts.get('callback', 0)
        )
        
        # Stage 4: Directly interested
        stage_4_interested = outcome_counts.get('interested', 0)
        
        # Calculate conversion rates
        contact_rate = (stage_2_contacted / stage_1_total * 100) if stage_1_total > 0 else 0
        positive_rate = (stage_3_positive / stage_2_contacted * 100) if stage_2_contacted > 0 else 0
        interest_rate = (stage_4_interested / stage_3_positive * 100) if stage_3_positive > 0 else 0
        overall_conversion = (stage_4_interested / stage_1_total * 100) if stage_1_total > 0 else 0
        
        funnel_stages = [
            {
                "stage": 1,
                "name": "Total Calls Attempted",
                "count": stage_1_total,
                "percentage": 100.0,
                "drop_off": 0
            },
            {
                "stage": 2,
                "name": "Successfully Contacted",
                "count": stage_2_contacted,
                "percentage": round(contact_rate, 2),
                "drop_off": stage_1_total - stage_2_contacted
            },
            {
                "stage": 3,
                "name": "Positive Response",
                "count": stage_3_positive,
                "percentage": round(positive_rate, 2),
                "drop_off": stage_2_contacted - stage_3_positive
            },
            {
                "stage": 4,
                "name": "Directly Interested",
                "count": stage_4_interested,
                "percentage": round(interest_rate, 2),
                "drop_off": stage_3_positive - stage_4_interested
            }
        ]
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "agent_id": agent_id,
            "funnel_stages": funnel_stages,
            "overall_conversion_rate": round(overall_conversion, 2),
            "key_insights": {
                "contact_rate": round(contact_rate, 2),
                "positive_response_rate": round(positive_rate, 2),
                "interest_conversion_rate": round(interest_rate, 2)
            }
        }
        
        # Cache for 1 hour
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching conversion funnel: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch conversion funnel: {str(e)}"
        )


@router.get("/analytics/call-duration")
async def get_call_duration_analytics(
    user_id: str = Query(...),
    date_range: str = Query("last_30_days"),
    agent_id: Optional[str] = Query(None)
) -> Dict[str, Any]:
    """
    Analyze call duration patterns
    
    Returns:
    - Average duration by outcome
    - Duration distribution
    - Optimal call length insights
    """
    try:
        cache_key = f"calls:duration_analytics:{user_id}:{date_range}:{agent_id}"
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
        
        if agent_id:
            query_filter["agent_id"] = agent_id
        
        # Average duration by outcome
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": "$outcome",
                "avg_duration": {"$avg": "$duration"},
                "min_duration": {"$min": "$duration"},
                "max_duration": {"$max": "$duration"},
                "count": {"$sum": 1}
            }}
        ]
        
        duration_cursor = db.cold_calls.aggregate(pipeline)
        duration_data = await duration_cursor.to_list(length=None)
        
        duration_by_outcome = [
            {
                "outcome": item['_id'],
                "avg_duration_seconds": round(item['avg_duration'], 2),
                "avg_duration_formatted": _format_duration(int(item['avg_duration'])),
                "min_duration_seconds": item['min_duration'],
                "max_duration_seconds": item['max_duration'],
                "call_count": item['count']
            }
            for item in duration_data
        ]
        
        # Sort by average duration
        duration_by_outcome.sort(key=lambda x: x['avg_duration_seconds'], reverse=True)
        
        # Duration distribution (buckets)
        buckets = [
            {"min": 0, "max": 30, "label": "0-30s"},
            {"min": 31, "max": 60, "label": "31-60s"},
            {"min": 61, "max": 120, "label": "1-2min"},
            {"min": 121, "max": 300, "label": "2-5min"},
            {"min": 301, "max": 600, "label": "5-10min"},
            {"min": 601, "max": 999999, "label": "10min+"}
        ]
        
        distribution = []
        for bucket in buckets:
            count = await db.cold_calls.count_documents({
                **query_filter,
                "duration": {"$gte": bucket['min'], "$lte": bucket['max']}
            })
            distribution.append({
                "range": bucket['label'],
                "count": count
            })
        
        # Get calls with best outcomes and their durations
        successful_calls_cursor = db.cold_calls.find({
            **query_filter,
            "outcome": "interested"
        })
        successful_calls = await successful_calls_cursor.to_list(length=None)
        
        if successful_calls:
            successful_durations = [call['duration'] for call in successful_calls]
            optimal_duration = sum(successful_durations) / len(successful_durations)
        else:
            optimal_duration = 0
        
        response = {
            "user_id": user_id,
            "date_range": date_range,
            "agent_id": agent_id,
            "duration_by_outcome": duration_by_outcome,
            "duration_distribution": distribution,
            "insights": {
                "optimal_duration_seconds": round(optimal_duration, 2),
                "optimal_duration_formatted": _format_duration(int(optimal_duration)),
                "recommendation": _get_duration_recommendation(optimal_duration)
            }
        }
        
        # Cache for 1 hour
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching call duration analytics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch call duration analytics: {str(e)}"
        )


@router.get("/analytics/trends")
async def get_trends_analysis(
    user_id: str = Query(...),
    metric: str = Query("success_rate", description="Metric: success_rate, call_volume, avg_duration, contact_rate")
) -> Dict[str, Any]:
    """
    Analyze trends over the past 90 days
    
    Returns:
    - Weekly trends for selected metric
    - Growth rate
    - Forecasted trend
    """
    try:
        cache_key = f"calls:trends:{user_id}:{metric}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Last 90 days
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)
        
        query_filter = {
            "user_id": user_id,
            "called_at": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        
        # Group by week
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-W%V",
                        "date": "$called_at"
                    }
                },
                "total_calls": {"$sum": 1},
                "total_duration": {"$sum": "$duration"},
                "successful_calls": {
                    "$sum": {
                        "$cond": [
                            {"$in": ["$outcome", ["interested", "callback"]]},
                            1,
                            0
                        ]
                    }
                },
                "contacted_calls": {
                    "$sum": {
                        "$cond": [
                            {"$nin": ["$outcome", ["voicemail", "no_answer"]]},
                            1,
                            0
                        ]
                    }
                }
            }},
            {"$sort": {"_id": 1}}
        ]
        
        trends_cursor = db.cold_calls.aggregate(pipeline)
        trends_data = await trends_cursor.to_list(length=None)
        
        # Calculate metric for each week
        weekly_trends = []
        for week in trends_data:
            total_calls = week['total_calls']
            
            if metric == "success_rate":
                value = (week['successful_calls'] / total_calls * 100) if total_calls > 0 else 0
            elif metric == "call_volume":
                value = total_calls
            elif metric == "avg_duration":
                value = (week['total_duration'] / total_calls) if total_calls > 0 else 0
            elif metric == "contact_rate":
                value = (week['contacted_calls'] / total_calls * 100) if total_calls > 0 else 0
            else:
                value = 0
            
            weekly_trends.append({
                "week": week['_id'],
                "value": round(value, 2)
            })
        
        # Calculate growth rate (first vs last week)
        if len(weekly_trends) >= 2:
            first_week_value = weekly_trends[0]['value']
            last_week_value = weekly_trends[-1]['value']
            growth_rate = ((last_week_value - first_week_value) / first_week_value * 100) if first_week_value > 0 else 0
        else:
            growth_rate = 0
        
        # Determine trend direction
        if growth_rate > 5:
            trend_direction = "increasing"
        elif growth_rate < -5:
            trend_direction = "decreasing"
        else:
            trend_direction = "stable"
        
        response = {
            "user_id": user_id,
            "metric": metric,
            "weekly_trends": weekly_trends,
            "analysis": {
                "growth_rate": round(growth_rate, 2),
                "trend_direction": trend_direction,
                "weeks_analyzed": len(weekly_trends)
            }
        }
        
        # Cache for 6 hours
        await redis_service.set(cache_key, response, ttl=21600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching trends analysis: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch trends analysis: {str(e)}"
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


def _format_duration(seconds: int) -> str:
    """Format duration in seconds to human readable format"""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"


def _get_duration_recommendation(optimal_duration: float) -> str:
    """Get recommendation based on optimal duration"""
    if optimal_duration < 60:
        return "Very short calls. Consider building more rapport before pitching."
    elif optimal_duration < 120:
        return "Good balance. Maintain current call length for best results."
    elif optimal_duration < 300:
        return "Longer calls show engagement. Ensure you're closing effectively."
    else:
        return "Very long calls. May indicate indecision - work on closing techniques."