"""
Cold Calling History - Call logs and detailed records
"""

from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from bson import ObjectId
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/history")
async def get_call_history(
    user_id: str = Query(...),
    agent_id: Optional[str] = Query(None, description="Filter by agent"),
    outcome: Optional[str] = Query(None, description="Filter by outcome: interested, not_interested, callback, voicemail, no_answer"),
    date_range: str = Query("last_7_days", description="Date range: today, yesterday, last_7_days, last_30_days, custom"),
    start_date: Optional[str] = Query(None, description="Start date for custom range (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for custom range (YYYY-MM-DD)"),
    search: Optional[str] = Query(None, description="Search by customer phone or notes"),
    sort_by: str = Query("called_at", description="Sort by: called_at, duration, agent_name"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200)
) -> Dict[str, Any]:
    """
    Get call history with filters and pagination
    
    Returns:
    - Call records
    - Agent information
    - Customer details
    - Call outcomes and notes
    - Duration and timestamps
    """
    try:
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range, start_date, end_date)
        
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
        
        if outcome:
            query_filter["outcome"] = outcome
        
        if search:
            query_filter["$or"] = [
                {"customer_phone": {"$regex": search, "$options": "i"}},
                {"notes": {"$regex": search, "$options": "i"}},
                {"customer_name": {"$regex": search, "$options": "i"}}
            ]
        
        # Count total calls
        total_count = await db.cold_calls.count_documents(query_filter)
        
        # Calculate pagination
        skip = (page - 1) * limit
        
        # Fetch calls
        sort_direction = -1 if order == "desc" else 1
        calls_cursor = db.cold_calls.find(query_filter).sort(sort_by, sort_direction).skip(skip).limit(limit)
        calls = await calls_cursor.to_list(length=limit)
        
        # Format calls
        formatted_calls = []
        for call in calls:
            formatted_calls.append({
                "id": str(call['_id']),
                "call_sid": call.get('call_sid'),
                "agent": {
                    "id": call.get('agent_id'),
                    "name": call.get('agent_name')
                },
                "customer": {
                    "phone": call.get('customer_phone'),
                    "name": call.get('customer_name')
                },
                "duration": call.get('duration'),
                "duration_formatted": _format_duration(call.get('duration', 0)),
                "outcome": call.get('outcome'),
                "notes": call.get('notes'),
                "follow_up_date": call.get('follow_up_date').isoformat() if call.get('follow_up_date') else None,
                "recording_url": call.get('recording_url'),
                "called_at": call.get('called_at').isoformat() if call.get('called_at') else None,
                "tags": call.get('tags', [])
            })
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        
        response = {
            "user_id": user_id,
            "filters": {
                "agent_id": agent_id,
                "outcome": outcome,
                "date_range": date_range,
                "search": search,
                "sort_by": sort_by,
                "order": order
            },
            "calls": formatted_calls,
            "pagination": {
                "page": page,
                "limit": limit,
                "total_calls": total_count,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching call history: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch call history: {str(e)}"
        )


@router.get("/history/{call_id}")
async def get_call_details(
    user_id: str = Query(...),
    call_id: str = ...
) -> Dict[str, Any]:
    """
    Get detailed information about a specific call
    
    Returns:
    - Complete call record
    - Recording URL if available
    - Transcript if available
    - Related follow-up calls
    """
    try:
        db = await get_database()
        
        call = await db.cold_calls.find_one({
            "_id": ObjectId(call_id),
            "user_id": user_id
        })
        
        if not call:
            raise HTTPException(
                status_code=404,
                detail="Call record not found"
            )
        
        # Get related calls (same customer)
        related_calls = []
        if call.get('customer_phone'):
            related_cursor = db.cold_calls.find({
                "user_id": user_id,
                "customer_phone": call['customer_phone'],
                "_id": {"$ne": ObjectId(call_id)}
            }).sort("called_at", -1).limit(5)
            
            related_list = await related_cursor.to_list(length=5)
            related_calls = [
                {
                    "id": str(c['_id']),
                    "agent_name": c.get('agent_name'),
                    "outcome": c.get('outcome'),
                    "duration": c.get('duration'),
                    "called_at": c.get('called_at').isoformat() if c.get('called_at') else None
                }
                for c in related_list
            ]
        
        response = {
            "id": str(call['_id']),
            "call_sid": call.get('call_sid'),
            "agent": {
                "id": call.get('agent_id'),
                "name": call.get('agent_name'),
                "email": call.get('agent_email')
            },
            "customer": {
                "phone": call.get('customer_phone'),
                "name": call.get('customer_name'),
                "company": call.get('customer_company'),
                "email": call.get('customer_email')
            },
            "call_details": {
                "duration": call.get('duration'),
                "duration_formatted": _format_duration(call.get('duration', 0)),
                "outcome": call.get('outcome'),
                "notes": call.get('notes'),
                "follow_up_date": call.get('follow_up_date').isoformat() if call.get('follow_up_date') else None,
                "recording_url": call.get('recording_url'),
                "transcript": call.get('transcript'),
                "tags": call.get('tags', [])
            },
            "timestamps": {
                "called_at": call.get('called_at').isoformat() if call.get('called_at') else None,
                "created_at": call.get('created_at').isoformat() if call.get('created_at') else None,
                "updated_at": call.get('updated_at').isoformat() if call.get('updated_at') else None
            },
            "related_calls": related_calls
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching call details: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch call details: {str(e)}"
        )


@router.post("/history")
async def create_call_record(
    user_id: str = Query(...),
    agent_id: str = Body(...),
    agent_name: str = Body(...),
    customer_phone: str = Body(...),
    customer_name: Optional[str] = Body(None),
    duration: int = Body(..., description="Duration in seconds"),
    outcome: str = Body(..., description="interested, not_interested, callback, voicemail, no_answer"),
    notes: Optional[str] = Body(None),
    follow_up_date: Optional[str] = Body(None, description="ISO datetime for follow-up"),
    tags: Optional[List[str]] = Body(None)
) -> Dict[str, Any]:
    """
    Create a new call record (manual entry or from integration)
    """
    try:
        db = await get_database()
        
        # Validate outcome
        valid_outcomes = ['interested', 'not_interested', 'callback', 'voicemail', 'no_answer']
        if outcome not in valid_outcomes:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid outcome. Must be one of: {', '.join(valid_outcomes)}"
            )
        
        # Parse follow-up date if provided
        follow_up_datetime = None
        if follow_up_date:
            try:
                follow_up_datetime = datetime.fromisoformat(follow_up_date)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid follow_up_date format. Use ISO format"
                )
        
        call_doc = {
            "user_id": user_id,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "customer_phone": customer_phone,
            "customer_name": customer_name,
            "duration": duration,
            "outcome": outcome,
            "notes": notes,
            "follow_up_date": follow_up_datetime,
            "tags": tags or [],
            "called_at": datetime.utcnow(),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        result = await db.cold_calls.insert_one(call_doc)
        
        # Invalidate cache
        await redis_service.delete_pattern(f"calls:overview:{user_id}:*")
        await redis_service.delete_pattern(f"calls:agents_summary:{user_id}:*")
        
        logger.info(f"Call record created: {result.inserted_id} by agent {agent_name}")
        
        return {
            "status": "success",
            "call_id": str(result.inserted_id),
            "message": "Call record created successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating call record: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create call record: {str(e)}"
        )


@router.patch("/history/{call_id}")
async def update_call_record(
    user_id: str = Query(...),
    call_id: str = ...,
    outcome: Optional[str] = Body(None),
    notes: Optional[str] = Body(None),
    follow_up_date: Optional[str] = Body(None),
    tags: Optional[List[str]] = Body(None)
) -> Dict[str, Any]:
    """
    Update a call record (notes, outcome, follow-up date)
    """
    try:
        db = await get_database()
        
        # Check if call exists
        call = await db.cold_calls.find_one({
            "_id": ObjectId(call_id),
            "user_id": user_id
        })
        
        if not call:
            raise HTTPException(
                status_code=404,
                detail="Call record not found"
            )
        
        # Build update document
        update_doc = {"updated_at": datetime.utcnow()}
        
        if outcome:
            valid_outcomes = ['interested', 'not_interested', 'callback', 'voicemail', 'no_answer']
            if outcome not in valid_outcomes:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid outcome. Must be one of: {', '.join(valid_outcomes)}"
                )
            update_doc["outcome"] = outcome
        
        if notes is not None:
            update_doc["notes"] = notes
        
        if follow_up_date:
            try:
                follow_up_datetime = datetime.fromisoformat(follow_up_date)
                update_doc["follow_up_date"] = follow_up_datetime
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid follow_up_date format"
                )
        
        if tags is not None:
            update_doc["tags"] = tags
        
        result = await db.cold_calls.update_one(
            {"_id": ObjectId(call_id)},
            {"$set": update_doc}
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"calls:overview:{user_id}:*")
        
        return {
            "status": "success",
            "call_id": call_id,
            "message": "Call record updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating call record: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update call record: {str(e)}"
        )


@router.delete("/history/{call_id}")
async def delete_call_record(
    user_id: str = Query(...),
    call_id: str = ...
) -> Dict[str, Any]:
    """
    Delete a call record
    """
    try:
        db = await get_database()
        
        result = await db.cold_calls.delete_one({
            "_id": ObjectId(call_id),
            "user_id": user_id
        })
        
        if result.deleted_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Call record not found"
            )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"calls:overview:{user_id}:*")
        await redis_service.delete_pattern(f"calls:agents_summary:{user_id}:*")
        
        return {
            "status": "success",
            "call_id": call_id,
            "message": "Call record deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting call record: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete call record: {str(e)}"
        )


@router.get("/history/followups")
async def get_follow_ups(
    user_id: str = Query(...),
    agent_id: Optional[str] = Query(None),
    status: str = Query("pending", description="pending, completed, overdue")
) -> Dict[str, Any]:
    """
    Get list of follow-up calls that need to be made
    
    Returns:
    - Scheduled follow-ups
    - Overdue follow-ups
    - Customer information
    - Previous call context
    """
    try:
        cache_key = f"calls:followups:{user_id}:{agent_id}:{status}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        now = datetime.utcnow()
        
        # Build query based on status
        query_filter = {
            "user_id": user_id,
            "outcome": "callback",
            "follow_up_date": {"$exists": True, "$ne": None}
        }
        
        if agent_id:
            query_filter["agent_id"] = agent_id
        
        if status == "pending":
            query_filter["follow_up_date"] = {"$gte": now}
        elif status == "overdue":
            query_filter["follow_up_date"] = {"$lt": now}
        
        # Fetch follow-ups
        followups_cursor = db.cold_calls.find(query_filter).sort("follow_up_date", 1)
        followups = await followups_cursor.to_list(length=None)
        
        formatted_followups = []
        for call in followups:
            follow_up_date = call.get('follow_up_date')
            time_until = (follow_up_date - now).total_seconds() / 3600 if follow_up_date else 0
            
            formatted_followups.append({
                "call_id": str(call['_id']),
                "customer": {
                    "phone": call.get('customer_phone'),
                    "name": call.get('customer_name')
                },
                "agent_name": call.get('agent_name'),
                "follow_up_date": follow_up_date.isoformat() if follow_up_date else None,
                "hours_until": round(time_until, 2) if time_until > 0 else 0,
                "is_overdue": time_until < 0,
                "previous_notes": call.get('notes'),
                "tags": call.get('tags', [])
            })
        
        response = {
            "user_id": user_id,
            "agent_id": agent_id,
            "status": status,
            "follow_ups": formatted_followups,
            "total_follow_ups": len(formatted_followups)
        }
        
        # Cache for 2 minutes
        await redis_service.set(cache_key, response, ttl=120)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching follow-ups: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch follow-ups: {str(e)}"
        )


def _calculate_date_range(date_range: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, datetime]:
    """Calculate start and end dates"""
    end = datetime.utcnow()
    
    if date_range == "custom" and start_date and end_date:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
    elif date_range == "today":
        start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_range == "yesterday":
        start = (end - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_range == "last_7_days":
        start = end - timedelta(days=7)
    elif date_range == "last_30_days":
        start = end - timedelta(days=30)
    else:
        start = end - timedelta(days=7)
    
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