"""
Inbox Messages - Unified messaging across all platforms
"""

from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.aggregators.inbox_aggregator import InboxAggregator
from bson import ObjectId
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/messages")
async def get_messages(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None, description="Filter by platform: facebook_messenger, instagram, whatsapp, twitter, email"),
    status: Optional[str] = Query(None, description="Filter by status: unread, read, replied, archived"),
    priority: Optional[str] = Query(None, description="Filter by priority: high, medium, low"),
    date_range: str = Query("last_7_days", description="Date range: today, last_7_days, last_30_days, custom"),
    start_date: Optional[str] = Query(None, description="Start date for custom range (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for custom range (YYYY-MM-DD)"),
    search: Optional[str] = Query(None, description="Search in message content"),
    sort_by: str = Query("timestamp", description="Sort by: timestamp, sender_name"),
    order: str = Query("desc", description="Order: asc, desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200)
) -> Dict[str, Any]:
    """
    Get unified inbox messages from all connected platforms
    
    Returns:
    - Messages from all platforms in unified format
    - Sender information
    - Platform-specific metadata
    - Read/unread status
    - Reply status
    """
    try:
        # Build cache key
        cache_key = f"inbox:messages:{user_id}:{platform}:{status}:{priority}:{date_range}:{search}:{page}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            logger.info(f"Cache hit for inbox messages: {user_id}")
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range, start_date, end_date)
        
        # Build query filter
        query_filter = {
            "user_id": user_id,
            "timestamp": {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        }
        
        if platform:
            query_filter["platform"] = platform
        
        if status:
            if status == "unread":
                query_filter["read"] = False
            elif status == "read":
                query_filter["read"] = True
            elif status == "replied":
                query_filter["replied"] = True
            elif status == "archived":
                query_filter["archived"] = True
        
        if priority:
            query_filter["priority"] = priority
        
        if search:
            query_filter["$or"] = [
                {"message": {"$regex": search, "$options": "i"}},
                {"sender.name": {"$regex": search, "$options": "i"}}
            ]
        
        # Count total messages
        total_count = await db.inbox_messages.count_documents(query_filter)
        
        # Calculate pagination
        skip = (page - 1) * limit
        
        # Fetch messages
        sort_direction = -1 if order == "desc" else 1
        messages_cursor = db.inbox_messages.find(query_filter).sort(sort_by, sort_direction).skip(skip).limit(limit)
        messages = await messages_cursor.to_list(length=limit)
        
        # Format messages
        formatted_messages = []
        for msg in messages:
            formatted_messages.append({
                "id": str(msg['_id']),
                "platform": msg.get('platform'),
                "message_id": msg.get('message_id'),
                "sender": {
                    "id": msg.get('sender', {}).get('id'),
                    "name": msg.get('sender', {}).get('name'),
                    "profile_pic": msg.get('sender', {}).get('profile_pic'),
                    "username": msg.get('sender', {}).get('username')
                },
                "message": msg.get('message'),
                "attachments": msg.get('attachments', []),
                "timestamp": msg.get('timestamp').isoformat() if msg.get('timestamp') else None,
                "read": msg.get('read', False),
                "replied": msg.get('replied', False),
                "archived": msg.get('archived', False),
                "priority": msg.get('priority', 'medium'),
                "tags": msg.get('tags', []),
                "metadata": msg.get('metadata', {})
            })
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        
        response = {
            "user_id": user_id,
            "filters": {
                "platform": platform,
                "status": status,
                "priority": priority,
                "date_range": date_range,
                "search": search
            },
            "messages": formatted_messages,
            "pagination": {
                "page": page,
                "limit": limit,
                "total_messages": total_count,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
        
        # Cache for 2 minutes (inbox data should be relatively fresh)
        await redis_service.set(cache_key, response, ttl=120)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching inbox messages: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch inbox messages: {str(e)}"
        )


@router.get("/messages/{message_id}")
async def get_message_details(
    user_id: str = Query(...),
    message_id: str = ...
) -> Dict[str, Any]:
    """
    Get detailed information about a specific message
    
    Returns:
    - Full message content
    - Conversation thread
    - All attachments
    - Sender profile
    - Reply history
    """
    try:
        db = await get_database()
        
        # Fetch message
        message = await db.inbox_messages.find_one({
            "_id": ObjectId(message_id),
            "user_id": user_id
        })
        
        if not message:
            raise HTTPException(
                status_code=404,
                detail="Message not found"
            )
        
        # Get conversation thread (if available)
        thread_id = message.get('thread_id') or message.get('sender', {}).get('id')
        
        thread_filter = {
            "user_id": user_id,
            "$or": [
                {"thread_id": thread_id},
                {"sender.id": thread_id}
            ]
        }
        
        thread_cursor = db.inbox_messages.find(thread_filter).sort("timestamp", 1)
        thread_messages = await thread_cursor.to_list(length=100)
        
        # Format thread
        formatted_thread = []
        for msg in thread_messages:
            formatted_thread.append({
                "id": str(msg['_id']),
                "message": msg.get('message'),
                "timestamp": msg.get('timestamp').isoformat() if msg.get('timestamp') else None,
                "is_from_sender": msg.get('_id') != message['_id'],  # Simple check
                "attachments": msg.get('attachments', [])
            })
        
        # Mark as read
        await db.inbox_messages.update_one(
            {"_id": ObjectId(message_id)},
            {"$set": {"read": True, "read_at": datetime.utcnow()}}
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"inbox:messages:{user_id}:*")
        await redis_service.delete_pattern(f"inbox:stats:{user_id}:*")
        
        response = {
            "id": str(message['_id']),
            "platform": message.get('platform'),
            "message_id": message.get('message_id'),
            "sender": {
                "id": message.get('sender', {}).get('id'),
                "name": message.get('sender', {}).get('name'),
                "profile_pic": message.get('sender', {}).get('profile_pic'),
                "username": message.get('sender', {}).get('username'),
                "email": message.get('sender', {}).get('email'),
                "phone": message.get('sender', {}).get('phone')
            },
            "message": message.get('message'),
            "attachments": message.get('attachments', []),
            "timestamp": message.get('timestamp').isoformat() if message.get('timestamp') else None,
            "read": True,
            "read_at": datetime.utcnow().isoformat(),
            "replied": message.get('replied', False),
            "priority": message.get('priority', 'medium'),
            "tags": message.get('tags', []),
            "metadata": message.get('metadata', {}),
            "conversation_thread": formatted_thread
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching message details: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch message details: {str(e)}"
        )


@router.patch("/messages/{message_id}/mark-read")
async def mark_message_as_read(
    user_id: str = Query(...),
    message_id: str = ...
) -> Dict[str, Any]:
    """
    Mark a message as read
    """
    try:
        db = await get_database()
        
        result = await db.inbox_messages.update_one(
            {"_id": ObjectId(message_id), "user_id": user_id},
            {"$set": {"read": True, "read_at": datetime.utcnow()}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Message not found or already marked as read"
            )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"inbox:messages:{user_id}:*")
        await redis_service.delete_pattern(f"inbox:stats:{user_id}:*")
        
        return {
            "status": "success",
            "message_id": message_id,
            "marked_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking message as read: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to mark message as read: {str(e)}"
        )


@router.patch("/messages/{message_id}/mark-unread")
async def mark_message_as_unread(
    user_id: str = Query(...),
    message_id: str = ...
) -> Dict[str, Any]:
    """
    Mark a message as unread
    """
    try:
        db = await get_database()
        
        result = await db.inbox_messages.update_one(
            {"_id": ObjectId(message_id), "user_id": user_id},
            {"$set": {"read": False}, "$unset": {"read_at": ""}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Message not found"
            )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"inbox:messages:{user_id}:*")
        await redis_service.delete_pattern(f"inbox:stats:{user_id}:*")
        
        return {
            "status": "success",
            "message_id": message_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking message as unread: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to mark message as unread: {str(e)}"
        )


@router.patch("/messages/bulk/mark-read")
async def bulk_mark_as_read(
    user_id: str = Query(...),
    message_ids: List[str] = Body(..., embed=True)
) -> Dict[str, Any]:
    """
    Mark multiple messages as read
    """
    try:
        db = await get_database()
        
        object_ids = [ObjectId(msg_id) for msg_id in message_ids]
        
        result = await db.inbox_messages.update_many(
            {"_id": {"$in": object_ids}, "user_id": user_id},
            {"$set": {"read": True, "read_at": datetime.utcnow()}}
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"inbox:messages:{user_id}:*")
        await redis_service.delete_pattern(f"inbox:stats:{user_id}:*")
        
        return {
            "status": "success",
            "marked_count": result.modified_count,
            "marked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error bulk marking messages as read: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to bulk mark messages as read: {str(e)}"
        )


@router.patch("/messages/{message_id}/archive")
async def archive_message(
    user_id: str = Query(...),
    message_id: str = ...
) -> Dict[str, Any]:
    """
    Archive a message
    """
    try:
        db = await get_database()
        
        result = await db.inbox_messages.update_one(
            {"_id": ObjectId(message_id), "user_id": user_id},
            {"$set": {"archived": True, "archived_at": datetime.utcnow()}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Message not found"
            )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"inbox:messages:{user_id}:*")
        await redis_service.delete_pattern(f"inbox:stats:{user_id}:*")
        
        return {
            "status": "success",
            "message_id": message_id,
            "archived_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error archiving message: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to archive message: {str(e)}"
        )


@router.patch("/messages/{message_id}/unarchive")
async def unarchive_message(
    user_id: str = Query(...),
    message_id: str = ...
) -> Dict[str, Any]:
    """
    Unarchive a message
    """
    try:
        db = await get_database()
        
        result = await db.inbox_messages.update_one(
            {"_id": ObjectId(message_id), "user_id": user_id},
            {"$set": {"archived": False}, "$unset": {"archived_at": ""}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Message not found"
            )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"inbox:messages:{user_id}:*")
        await redis_service.delete_pattern(f"inbox:stats:{user_id}:*")
        
        return {
            "status": "success",
            "message_id": message_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error unarchiving message: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to unarchive message: {str(e)}"
        )


@router.post("/messages/{message_id}/reply")
async def reply_to_message(
    user_id: str = Query(...),
    message_id: str = ...,
    reply_text: str = Body(..., embed=True),
    attachments: Optional[List[str]] = Body(None, embed=True)
) -> Dict[str, Any]:
    """
    Reply to a message (this will call the appropriate platform API)
    
    Note: This requires platform-specific implementation
    """
    try:
        db = await get_database()
        
        # Get original message
        original_message = await db.inbox_messages.find_one({
            "_id": ObjectId(message_id),
            "user_id": user_id
        })
        
        if not original_message:
            raise HTTPException(
                status_code=404,
                detail="Message not found"
            )
        
        platform = original_message.get('platform')
        
        # TODO: Call appropriate platform API to send reply
        # For now, just mark as replied and create a reply record
        
        # Mark original message as replied
        await db.inbox_messages.update_one(
            {"_id": ObjectId(message_id)},
            {"$set": {"replied": True, "replied_at": datetime.utcnow()}}
        )
        
        # Create reply record (for tracking)
        reply_doc = {
            "user_id": user_id,
            "platform": platform,
            "original_message_id": message_id,
            "reply_text": reply_text,
            "attachments": attachments or [],
            "sent_at": datetime.utcnow(),
            "status": "sent"  # TODO: Update based on actual API response
        }
        
        reply_result = await db.inbox_replies.insert_one(reply_doc)
        
        # Invalidate cache
        await redis_service.delete_pattern(f"inbox:messages:{user_id}:*")
        await redis_service.delete_pattern(f"inbox:stats:{user_id}:*")
        
        logger.info(f"Reply sent for message {message_id} on platform {platform}")
        
        return {
            "status": "success",
            "message_id": message_id,
            "reply_id": str(reply_result.inserted_id),
            "platform": platform,
            "sent_at": datetime.utcnow().isoformat(),
            "note": "Platform-specific sending implementation required"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error replying to message: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reply to message: {str(e)}"
        )


@router.patch("/messages/{message_id}/priority")
async def update_message_priority(
    user_id: str = Query(...),
    message_id: str = ...,
    priority: str = Body(..., embed=True, description="Priority: high, medium, low")
) -> Dict[str, Any]:
    """
    Update message priority
    """
    try:
        if priority not in ["high", "medium", "low"]:
            raise HTTPException(
                status_code=400,
                detail="Invalid priority. Must be: high, medium, or low"
            )
        
        db = await get_database()
        
        result = await db.inbox_messages.update_one(
            {"_id": ObjectId(message_id), "user_id": user_id},
            {"$set": {"priority": priority}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Message not found"
            )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"inbox:messages:{user_id}:*")
        
        return {
            "status": "success",
            "message_id": message_id,
            "priority": priority
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating message priority: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update message priority: {str(e)}"
        )


@router.patch("/messages/{message_id}/tags")
async def update_message_tags(
    user_id: str = Query(...),
    message_id: str = ...,
    tags: List[str] = Body(..., embed=True)
) -> Dict[str, Any]:
    """
    Add tags to a message
    """
    try:
        db = await get_database()
        
        result = await db.inbox_messages.update_one(
            {"_id": ObjectId(message_id), "user_id": user_id},
            {"$set": {"tags": tags}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Message not found"
            )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"inbox:messages:{user_id}:*")
        
        return {
            "status": "success",
            "message_id": message_id,
            "tags": tags
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating message tags: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update message tags: {str(e)}"
        )


@router.get("/stats")
async def get_inbox_stats(
    user_id: str = Query(...),
    date_range: str = Query("today", description="Date range: today, last_7_days, last_30_days")
) -> Dict[str, Any]:
    """
    Get inbox statistics
    
    Returns:
    - Total messages
    - Unread count
    - Platform breakdown
    - Response time metrics
    - Peak hours
    """
    try:
        cache_key = f"inbox:stats:{user_id}:{date_range}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Calculate date range
        dates = _calculate_date_range(date_range)
        
        query_filter = {
            "user_id": user_id,
            "timestamp": {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        }
        
        # Get statistics using aggregation
        pipeline = [
            {"$match": query_filter},
            {"$facet": {
                "total_count": [{"$count": "count"}],
                "unread_count": [
                    {"$match": {"read": False}},
                    {"$count": "count"}
                ],
                "platform_breakdown": [
                    {"$group": {
                        "_id": "$platform",
                        "count": {"$sum": 1}
                    }}
                ],
                "priority_breakdown": [
                    {"$group": {
                        "_id": "$priority",
                        "count": {"$sum": 1}
                    }}
                ],
                "hourly_distribution": [
                    {"$group": {
                        "_id": {"$hour": "$timestamp"},
                        "count": {"$sum": 1}
                    }},
                    {"$sort": {"_id": 1}}
                ]
            }}
        ]
        
        stats_cursor = db.inbox_messages.aggregate(pipeline)
        stats_result = await stats_cursor.to_list(length=1)
        
        if not stats_result:
            return {
                "user_id": user_id,
                "date_range": date_range,
                "total_messages": 0,
                "unread_count": 0
            }
        
        stats = stats_result[0]
        
        # Format response
        total_count = stats['total_count'][0]['count'] if stats['total_count'] else 0
        unread_count = stats['unread_count'][0]['count'] if stats['unread_count'] else 0
        
        platform_breakdown = {
            item['_id']: item['count'] 
            for item in stats['platform_breakdown']
        }
        
        priority_breakdown = {
            item['_id']: item['count'] 
            for item in stats['priority_breakdown']
        }
        
        hourly_distribution = [
            {"hour": item['_id'], "count": item['count']}
            for item in stats['hourly_distribution']
        ]
        
        response = {
            "user_id": user_id,
            "date_range": {
                "label": date_range,
                "start_date": dates['start_date'].isoformat(),
                "end_date": dates['end_date'].isoformat()
            },
            "summary": {
                "total_messages": total_count,
                "unread_count": unread_count,
                "read_count": total_count - unread_count,
                "read_percentage": round((total_count - unread_count) / total_count * 100, 2) if total_count > 0 else 0
            },
            "platform_breakdown": platform_breakdown,
            "priority_breakdown": priority_breakdown,
            "hourly_distribution": hourly_distribution
        }
        
        # Cache for 5 minutes
        await redis_service.set(cache_key, response, ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching inbox stats: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch inbox stats: {str(e)}"
        )


def _calculate_date_range(date_range: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, datetime]:
    """Calculate start and end dates"""
    end = datetime.utcnow()
    
    if date_range == "custom" and start_date and end_date:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
    elif date_range == "today":
        start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_range == "last_7_days":
        start = end - timedelta(days=7)
    elif date_range == "last_30_days":
        start = end - timedelta(days=30)
    else:
        start = end - timedelta(days=7)  # Default
    
    return {
        "start_date": start,
        "end_date": end
    }