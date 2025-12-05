"""
Email Marketing Scheduled Campaigns - Schedule management and automation
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


@router.get("/scheduled")
async def get_scheduled_campaigns(
    user_id: str = Query(...),
    status: str = Query("pending", description="Status: pending, completed, cancelled"),
    sort_by: str = Query("scheduled_at", description="Sort by: scheduled_at, campaign_name"),
    order: str = Query("asc"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get list of scheduled campaigns
    
    Returns:
    - Scheduled campaigns with send times
    - Campaign details
    - Recipient information
    - Scheduling status
    """
    try:
        cache_key = f"email:scheduled:{user_id}:{status}:{page}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            logger.info(f"Cache hit for scheduled campaigns: {user_id}")
            return cached_data
        
        db = await get_database()
        
        # Build query filter
        query_filter = {
            "user_id": user_id,
            "schedule_status": status
        }
        
        # Count total
        total_count = await db.email_scheduled.count_documents(query_filter)
        
        # Calculate pagination
        skip = (page - 1) * limit
        
        # Fetch scheduled campaigns
        sort_direction = -1 if order == "desc" else 1
        scheduled_cursor = db.email_scheduled.find(query_filter).sort(sort_by, sort_direction).skip(skip).limit(limit)
        scheduled_campaigns = await scheduled_cursor.to_list(length=limit)
        
        # Format response
        formatted_campaigns = []
        for schedule in scheduled_campaigns:
            # Get campaign details
            campaign = await db.email_campaigns.find_one({"_id": ObjectId(schedule['campaign_id'])})
            
            formatted_campaigns.append({
                "id": str(schedule['_id']),
                "campaign_id": schedule['campaign_id'],
                "campaign_name": campaign.get('campaign_name') if campaign else "Unknown",
                "campaign_subject": campaign.get('subject') if campaign else None,
                "scheduled_at": schedule.get('scheduled_at').isoformat() if schedule.get('scheduled_at') else None,
                "timezone": schedule.get('timezone', 'UTC'),
                "recipient_count": schedule.get('recipient_count', 0),
                "schedule_status": schedule.get('schedule_status'),
                "created_at": schedule.get('created_at').isoformat() if schedule.get('created_at') else None,
                "created_by": schedule.get('created_by'),
                "send_options": schedule.get('send_options', {})
            })
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        
        response = {
            "user_id": user_id,
            "filters": {
                "status": status,
                "sort_by": sort_by,
                "order": order
            },
            "scheduled_campaigns": formatted_campaigns,
            "pagination": {
                "page": page,
                "limit": limit,
                "total_scheduled": total_count,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
        
        # Cache for 2 minutes
        await redis_service.set(cache_key, response, ttl=120)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching scheduled campaigns: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch scheduled campaigns: {str(e)}"
        )


@router.post("/scheduled")
async def schedule_campaign(
    user_id: str = Query(...),
    campaign_id: str = Body(..., embed=True),
    scheduled_at: str = Body(..., embed=True, description="ISO datetime when to send"),
    timezone: str = Body("UTC", embed=True),
    recipient_list_id: Optional[str] = Body(None, embed=True),
    send_options: Optional[Dict[str, Any]] = Body(None, embed=True)
) -> Dict[str, Any]:
    """
    Schedule a campaign for future sending
    
    Send options can include:
    - send_test_first: bool
    - batch_size: int (for throttling)
    - resend_to_unopened: bool
    """
    try:
        db = await get_database()
        
        # Validate campaign exists
        campaign = await db.email_campaigns.find_one({
            "_id": ObjectId(campaign_id),
            "user_id": user_id
        })
        
        if not campaign:
            raise HTTPException(
                status_code=404,
                detail="Campaign not found"
            )
        
        # Validate campaign is ready to send
        if campaign.get('status') not in ['draft', 'scheduled']:
            raise HTTPException(
                status_code=400,
                detail="Campaign has already been sent"
            )
        
        if not campaign.get('subject') or not campaign.get('html_content'):
            raise HTTPException(
                status_code=400,
                detail="Campaign must have subject and content before scheduling"
            )
        
        # Parse scheduled datetime
        try:
            scheduled_datetime = datetime.fromisoformat(scheduled_at)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS)"
            )
        
        # Validate scheduled time is in the future
        if scheduled_datetime <= datetime.utcnow():
            raise HTTPException(
                status_code=400,
                detail="Scheduled time must be in the future"
            )
        
        # TODO: Get recipient count from list
        recipient_count = 0
        if recipient_list_id:
            # recipient_count = await get_list_count(recipient_list_id)
            recipient_count = 1000  # Placeholder
        
        # Create schedule document
        schedule_doc = {
            "user_id": user_id,
            "campaign_id": campaign_id,
            "scheduled_at": scheduled_datetime,
            "timezone": timezone,
            "recipient_list_id": recipient_list_id,
            "recipient_count": recipient_count,
            "send_options": send_options or {},
            "schedule_status": "pending",
            "created_at": datetime.utcnow(),
            "created_by": user_id
        }
        
        result = await db.email_scheduled.insert_one(schedule_doc)
        
        # Update campaign status to scheduled
        await db.email_campaigns.update_one(
            {"_id": ObjectId(campaign_id)},
            {
                "$set": {
                    "status": "scheduled",
                    "scheduled_at": scheduled_datetime,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"email:scheduled:{user_id}:*")
        await redis_service.delete_pattern(f"email:campaigns:{user_id}:*")
        
        logger.info(f"Campaign {campaign_id} scheduled for {scheduled_datetime} by user {user_id}")
        
        return {
            "status": "success",
            "schedule_id": str(result.inserted_id),
            "campaign_id": campaign_id,
            "scheduled_at": scheduled_datetime.isoformat(),
            "timezone": timezone,
            "message": "Campaign scheduled successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error scheduling campaign: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to schedule campaign: {str(e)}"
        )


@router.get("/scheduled/{schedule_id}")
async def get_schedule_details(
    user_id: str = Query(...),
    schedule_id: str = ...
) -> Dict[str, Any]:
    """
    Get detailed information about a scheduled campaign
    """
    try:
        db = await get_database()
        
        schedule = await db.email_scheduled.find_one({
            "_id": ObjectId(schedule_id),
            "user_id": user_id
        })
        
        if not schedule:
            raise HTTPException(
                status_code=404,
                detail="Scheduled campaign not found"
            )
        
        # Get campaign details
        campaign = await db.email_campaigns.find_one({"_id": ObjectId(schedule['campaign_id'])})
        
        response = {
            "id": str(schedule['_id']),
            "campaign": {
                "id": schedule['campaign_id'],
                "name": campaign.get('campaign_name') if campaign else "Unknown",
                "subject": campaign.get('subject') if campaign else None,
                "type": campaign.get('campaign_type') if campaign else None
            },
            "schedule_details": {
                "scheduled_at": schedule.get('scheduled_at').isoformat() if schedule.get('scheduled_at') else None,
                "timezone": schedule.get('timezone'),
                "recipient_count": schedule.get('recipient_count'),
                "recipient_list_id": schedule.get('recipient_list_id'),
                "send_options": schedule.get('send_options', {}),
                "schedule_status": schedule.get('schedule_status')
            },
            "metadata": {
                "created_at": schedule.get('created_at').isoformat() if schedule.get('created_at') else None,
                "created_by": schedule.get('created_by'),
                "sent_at": schedule.get('sent_at').isoformat() if schedule.get('sent_at') else None,
                "cancelled_at": schedule.get('cancelled_at').isoformat() if schedule.get('cancelled_at') else None,
                "cancelled_by": schedule.get('cancelled_by')
            }
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching schedule details: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch schedule details: {str(e)}"
        )


@router.patch("/scheduled/{schedule_id}")
async def update_schedule(
    user_id: str = Query(...),
    schedule_id: str = ...,
    scheduled_at: Optional[str] = Body(None, embed=True),
    timezone: Optional[str] = Body(None, embed=True),
    send_options: Optional[Dict[str, Any]] = Body(None, embed=True)
) -> Dict[str, Any]:
    """
    Update a scheduled campaign (only pending schedules can be updated)
    """
    try:
        db = await get_database()
        
        # Check if schedule exists and is pending
        schedule = await db.email_scheduled.find_one({
            "_id": ObjectId(schedule_id),
            "user_id": user_id
        })
        
        if not schedule:
            raise HTTPException(
                status_code=404,
                detail="Scheduled campaign not found"
            )
        
        if schedule.get('schedule_status') != 'pending':
            raise HTTPException(
                status_code=400,
                detail="Only pending schedules can be updated"
            )
        
        # Build update document
        update_doc = {"updated_at": datetime.utcnow()}
        
        if scheduled_at:
            try:
                scheduled_datetime = datetime.fromisoformat(scheduled_at)
                if scheduled_datetime <= datetime.utcnow():
                    raise HTTPException(
                        status_code=400,
                        detail="Scheduled time must be in the future"
                    )
                update_doc["scheduled_at"] = scheduled_datetime
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid datetime format"
                )
        
        if timezone:
            update_doc["timezone"] = timezone
        
        if send_options is not None:
            update_doc["send_options"] = send_options
        
        result = await db.email_scheduled.update_one(
            {"_id": ObjectId(schedule_id)},
            {"$set": update_doc}
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"email:scheduled:{user_id}:*")
        
        return {
            "status": "success",
            "schedule_id": schedule_id,
            "message": "Schedule updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating schedule: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update schedule: {str(e)}"
        )


@router.delete("/scheduled/{schedule_id}")
async def cancel_scheduled_campaign(
    user_id: str = Query(...),
    schedule_id: str = ...
) -> Dict[str, Any]:
    """
    Cancel a scheduled campaign
    """
    try:
        db = await get_database()
        
        # Check if schedule exists
        schedule = await db.email_scheduled.find_one({
            "_id": ObjectId(schedule_id),
            "user_id": user_id
        })
        
        if not schedule:
            raise HTTPException(
                status_code=404,
                detail="Scheduled campaign not found"
            )
        
        if schedule.get('schedule_status') != 'pending':
            raise HTTPException(
                status_code=400,
                detail="Only pending schedules can be cancelled"
            )
        
        # Update schedule status
        await db.email_scheduled.update_one(
            {"_id": ObjectId(schedule_id)},
            {
                "$set": {
                    "schedule_status": "cancelled",
                    "cancelled_at": datetime.utcnow(),
                    "cancelled_by": user_id
                }
            }
        )
        
        # Update campaign status back to draft
        await db.email_campaigns.update_one(
            {"_id": ObjectId(schedule['campaign_id'])},
            {
                "$set": {
                    "status": "draft",
                    "updated_at": datetime.utcnow()
                },
                "$unset": {"scheduled_at": ""}
            }
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"email:scheduled:{user_id}:*")
        await redis_service.delete_pattern(f"email:campaigns:{user_id}:*")
        
        logger.info(f"Scheduled campaign {schedule_id} cancelled by user {user_id}")
        
        return {
            "status": "success",
            "schedule_id": schedule_id,
            "message": "Scheduled campaign cancelled successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling schedule: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel schedule: {str(e)}"
        )


@router.get("/scheduled/upcoming")
async def get_upcoming_sends(
    user_id: str = Query(...),
    hours: int = Query(24, ge=1, le=168, description="Get campaigns scheduled within next N hours")
) -> Dict[str, Any]:
    """
    Get campaigns scheduled to send in the near future
    """
    try:
        cache_key = f"email:upcoming:{user_id}:{hours}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate time range
        now = datetime.utcnow()
        future_time = now + timedelta(hours=hours)
        
        query_filter = {
            "user_id": user_id,
            "schedule_status": "pending",
            "scheduled_at": {
                "$gte": now,
                "$lte": future_time
            }
        }
        
        # Fetch upcoming sends
        scheduled_cursor = db.email_scheduled.find(query_filter).sort("scheduled_at", 1)
        upcoming = await scheduled_cursor.to_list(length=None)
        
        # Format response
        formatted_upcoming = []
        for schedule in upcoming:
            campaign = await db.email_campaigns.find_one({"_id": ObjectId(schedule['campaign_id'])})
            
            # Calculate time until send
            time_until = schedule['scheduled_at'] - now
            hours_until = time_until.total_seconds() / 3600
            
            formatted_upcoming.append({
                "schedule_id": str(schedule['_id']),
                "campaign_id": schedule['campaign_id'],
                "campaign_name": campaign.get('campaign_name') if campaign else "Unknown",
                "subject": campaign.get('subject') if campaign else None,
                "scheduled_at": schedule['scheduled_at'].isoformat(),
                "hours_until_send": round(hours_until, 2),
                "recipient_count": schedule.get('recipient_count', 0)
            })
        
        response = {
            "user_id": user_id,
            "time_window_hours": hours,
            "upcoming_sends": formatted_upcoming,
            "total_upcoming": len(formatted_upcoming)
        }
        
        # Cache for 5 minutes
        await redis_service.set(cache_key, response, ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching upcoming sends: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch upcoming sends: {str(e)}"
        )


@router.get("/scheduled/calendar")
async def get_send_calendar(
    user_id: str = Query(...),
    month: Optional[int] = Query(None, ge=1, le=12, description="Month (1-12)"),
    year: Optional[int] = Query(None, ge=2020, le=2100, description="Year")
) -> Dict[str, Any]:
    """
    Get calendar view of scheduled campaigns for a specific month
    """
    try:
        # Use current month if not specified
        now = datetime.utcnow()
        target_month = month or now.month
        target_year = year or now.year
        
        cache_key = f"email:calendar:{user_id}:{target_month}:{target_year}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate month range
        start_date = datetime(target_year, target_month, 1)
        if target_month == 12:
            end_date = datetime(target_year + 1, 1, 1)
        else:
            end_date = datetime(target_year, target_month + 1, 1)
        
        query_filter = {
            "user_id": user_id,
            "schedule_status": "pending",
            "scheduled_at": {
                "$gte": start_date,
                "$lt": end_date
            }
        }
        
        # Fetch scheduled campaigns for this month
        scheduled_cursor = db.email_scheduled.find(query_filter).sort("scheduled_at", 1)
        scheduled_campaigns = await scheduled_cursor.to_list(length=None)
        
        # Group by date
        calendar_data = {}
        for schedule in scheduled_campaigns:
            campaign = await db.email_campaigns.find_one({"_id": ObjectId(schedule['campaign_id'])})
            
            date_key = schedule['scheduled_at'].strftime("%Y-%m-%d")
            
            if date_key not in calendar_data:
                calendar_data[date_key] = []
            
            calendar_data[date_key].append({
                "schedule_id": str(schedule['_id']),
                "campaign_id": schedule['campaign_id'],
                "campaign_name": campaign.get('campaign_name') if campaign else "Unknown",
                "scheduled_time": schedule['scheduled_at'].strftime("%H:%M"),
                "recipient_count": schedule.get('recipient_count', 0)
            })
        
        response = {
            "user_id": user_id,
            "month": target_month,
            "year": target_year,
            "calendar": calendar_data,
            "total_scheduled": len(scheduled_campaigns)
        }
        
        # Cache for 10 minutes
        await redis_service.set(cache_key, response, ttl=600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching send calendar: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch send calendar: {str(e)}"
        )


@router.get("/scheduled/stats")
async def get_scheduling_stats(
    user_id: str = Query(...)
) -> Dict[str, Any]:
    """
    Get statistics about scheduled campaigns
    """
    try:
        cache_key = f"email:scheduling_stats:{user_id}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Count by status
        pipeline = [
            {"$match": {"user_id": user_id}},
            {"$group": {
                "_id": "$schedule_status",
                "count": {"$sum": 1}
            }}
        ]
        
        stats_cursor = db.email_scheduled.aggregate(pipeline)
        stats_result = await stats_cursor.to_list(length=None)
        
        status_counts = {item['_id']: item['count'] for item in stats_result}
        
        # Get next scheduled send
        next_send = await db.email_scheduled.find_one(
            {
                "user_id": user_id,
                "schedule_status": "pending",
                "scheduled_at": {"$gte": datetime.utcnow()}
            },
            sort=[("scheduled_at", 1)]
        )
        
        next_send_info = None
        if next_send:
            campaign = await db.email_campaigns.find_one({"_id": ObjectId(next_send['campaign_id'])})
            next_send_info = {
                "campaign_name": campaign.get('campaign_name') if campaign else "Unknown",
                "scheduled_at": next_send['scheduled_at'].isoformat(),
                "recipient_count": next_send.get('recipient_count', 0)
            }
        
        response = {
            "user_id": user_id,
            "status_counts": {
                "pending": status_counts.get('pending', 0),
                "completed": status_counts.get('completed', 0),
                "cancelled": status_counts.get('cancelled', 0)
            },
            "next_scheduled_send": next_send_info
        }
        
        # Cache for 5 minutes
        await redis_service.set(cache_key, response, ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching scheduling stats: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch scheduling stats: {str(e)}"
        )


@router.post("/scheduled/{schedule_id}/send-now")
async def send_scheduled_campaign_now(
    user_id: str = Query(...),
    schedule_id: str = ...
) -> Dict[str, Any]:
    """
    Send a scheduled campaign immediately instead of waiting
    """
    try:
        db = await get_database()
        
        # Check if schedule exists
        schedule = await db.email_scheduled.find_one({
            "_id": ObjectId(schedule_id),
            "user_id": user_id
        })
        
        if not schedule:
            raise HTTPException(
                status_code=404,
                detail="Scheduled campaign not found"
            )
        
        if schedule.get('schedule_status') != 'pending':
            raise HTTPException(
                status_code=400,
                detail="Only pending schedules can be sent now"
            )
        
        # TODO: Integrate with email service provider to send immediately
        
        # Update schedule status
        await db.email_scheduled.update_one(
            {"_id": ObjectId(schedule_id)},
            {
                "$set": {
                    "schedule_status": "completed",
                    "sent_at": datetime.utcnow()
                }
            }
        )
        
        # Update campaign status
        await db.email_campaigns.update_one(
            {"_id": ObjectId(schedule['campaign_id'])},
            {
                "$set": {
                    "status": "sent",
                    "sent_at": datetime.utcnow()
                }
            }
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"email:scheduled:{user_id}:*")
        await redis_service.delete_pattern(f"email:campaigns:{user_id}:*")
        
        logger.info(f"Scheduled campaign {schedule_id} sent immediately by user {user_id}")
        
        return {
            "status": "success",
            "schedule_id": schedule_id,
            "campaign_id": schedule['campaign_id'],
            "message": "Campaign sent immediately",
            "sent_at": datetime.utcnow().isoformat(),
            "note": "Email service provider integration required for actual sending"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending scheduled campaign now: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send scheduled campaign: {str(e)}"
        )