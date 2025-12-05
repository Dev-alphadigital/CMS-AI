"""
Inbox Filters - Advanced filtering and search capabilities
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


@router.get("/filters/platforms")
async def get_platform_filters(
    user_id: str = Query(...)
) -> Dict[str, Any]:
    """
    Get available platform filters with message counts
    """
    try:
        cache_key = f"inbox:filters:platforms:{user_id}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Get platform counts
        pipeline = [
            {"$match": {"user_id": user_id}},
            {"$group": {
                "_id": "$platform",
                "total_count": {"$sum": 1},
                "unread_count": {
                    "$sum": {"$cond": [{"$eq": ["$read", False]}, 1, 0]}
                }
            }},
            {"$sort": {"total_count": -1}}
        ]
        
        platforms_cursor = db.inbox_messages.aggregate(pipeline)
        platforms = await platforms_cursor.to_list(length=None)
        
        formatted_platforms = [
            {
                "platform": p['_id'],
                "total_messages": p['total_count'],
                "unread_messages": p['unread_count']
            }
            for p in platforms
        ]
        
        response = {
            "user_id": user_id,
            "platforms": formatted_platforms,
            "total_platforms": len(formatted_platforms)
        }
        
        await redis_service.set(cache_key, response, ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching platform filters: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch platform filters: {str(e)}"
        )


@router.get("/filters/tags")
async def get_tag_filters(
    user_id: str = Query(...)
) -> Dict[str, Any]:
    """
    Get all tags used in messages
    """
    try:
        cache_key = f"inbox:filters:tags:{user_id}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Get distinct tags
        tags = await db.inbox_messages.distinct("tags", {"user_id": user_id})
        
        # Get count for each tag
        tag_counts = []
        for tag in tags:
            count = await db.inbox_messages.count_documents({
                "user_id": user_id,
                "tags": tag
            })
            tag_counts.append({
                "tag": tag,
                "count": count
            })
        
        # Sort by count
        tag_counts.sort(key=lambda x: x['count'], reverse=True)
        
        response = {
            "user_id": user_id,
            "tags": tag_counts,
            "total_tags": len(tag_counts)
        }
        
        await redis_service.set(cache_key, response, ttl=600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching tag filters: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch tag filters: {str(e)}"
        )


@router.get("/filters/senders")
async def get_sender_filters(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200)
) -> Dict[str, Any]:
    """
    Get top senders (most messages)
    """
    try:
        cache_key = f"inbox:filters:senders:{user_id}:{platform}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Build match filter
        match_filter = {"user_id": user_id}
        if platform:
            match_filter["platform"] = platform
        
        # Aggregate by sender
        pipeline = [
            {"$match": match_filter},
            {"$group": {
                "_id": {
                    "sender_id": "$sender.id",
                    "sender_name": "$sender.name",
                    "platform": "$platform"
                },
                "message_count": {"$sum": 1},
                "unread_count": {
                    "$sum": {"$cond": [{"$eq": ["$read", False]}, 1, 0]}
                },
                "last_message_at": {"$max": "$timestamp"}
            }},
            {"$sort": {"message_count": -1}},
            {"$limit": limit}
        ]
        
        senders_cursor = db.inbox_messages.aggregate(pipeline)
        senders = await senders_cursor.to_list(length=None)
        
        formatted_senders = [
            {
                "sender_id": s['_id']['sender_id'],
                "sender_name": s['_id']['sender_name'],
                "platform": s['_id']['platform'],
                "message_count": s['message_count'],
                "unread_count": s['unread_count'],
                "last_message_at": s['last_message_at'].isoformat() if s['last_message_at'] else None
            }
            for s in senders
        ]
        
        response = {
            "user_id": user_id,
            "platform_filter": platform,
            "senders": formatted_senders,
            "total_senders": len(formatted_senders)
        }
        
        await redis_service.set(cache_key, response, ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching sender filters: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch sender filters: {str(e)}"
        )


@router.get("/search")
async def search_messages(
    user_id: str = Query(...),
    query: str = Query(..., min_length=2),
    platform: Optional[str] = Query(None),
    sender: Optional[str] = Query(None, description="Sender ID"),
    date_from: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=200)
) -> Dict[str, Any]:
    """
    Advanced message search with full-text search
    
    Searches in:
    - Message content
    - Sender name
    - Tags
    """
    try:
        db = await get_database()
        
        # Build search filter
        search_filter = {
            "user_id": user_id,
            "$or": [
                {"message": {"$regex": query, "$options": "i"}},
                {"sender.name": {"$regex": query, "$options": "i"}},
                {"tags": {"$regex": query, "$options": "i"}}
            ]
        }
        
        if platform:
            search_filter["platform"] = platform
        
        if sender:
            search_filter["sender.id"] = sender
        
        if date_from or date_to:
            date_filter = {}
            if date_from:
                date_filter["$gte"] = datetime.fromisoformat(date_from)
            if date_to:
                date_filter["$lte"] = datetime.fromisoformat(date_to)
            search_filter["timestamp"] = date_filter
        
        # Execute search
        messages_cursor = db.inbox_messages.find(search_filter).sort("timestamp", -1).limit(limit)
        messages = await messages_cursor.to_list(length=limit)
        
        # Count total results
        total_count = await db.inbox_messages.count_documents(search_filter)
        
        # Format results
        formatted_messages = []
        for msg in messages:
            formatted_messages.append({
                "id": str(msg['_id']),
                "platform": msg.get('platform'),
                "sender": {
                    "id": msg.get('sender', {}).get('id'),
                    "name": msg.get('sender', {}).get('name')
                },
                "message": msg.get('message'),
                "timestamp": msg.get('timestamp').isoformat() if msg.get('timestamp') else None,
                "read": msg.get('read', False),
                "tags": msg.get('tags', [])
            })
        
        response = {
            "user_id": user_id,
            "query": query,
            "filters": {
                "platform": platform,
                "sender": sender,
                "date_from": date_from,
                "date_to": date_to
            },
            "results": formatted_messages,
            "total_results": total_count,
            "showing": len(formatted_messages)
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error searching messages: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to search messages: {str(e)}"
        )


@router.get("/conversations")
async def get_conversations(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get list of conversations (grouped by sender)
    
    Returns:
    - Latest message from each conversation
    - Unread count per conversation
    - Last activity timestamp
    """
    try:
        cache_key = f"inbox:conversations:{user_id}:{platform}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Build match filter
        match_filter = {"user_id": user_id}
        if platform:
            match_filter["platform"] = platform
        
        # Aggregate conversations
        pipeline = [
            {"$match": match_filter},
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": {
                    "sender_id": "$sender.id",
                    "platform": "$platform"
                },
                "sender_name": {"$first": "$sender.name"},
                "sender_profile_pic": {"$first": "$sender.profile_pic"},
                "last_message": {"$first": "$message"},
                "last_message_id": {"$first": "$_id"},
                "last_timestamp": {"$first": "$timestamp"},
                "unread_count": {
                    "$sum": {"$cond": [{"$eq": ["$read", False]}, 1, 0]}
                },
                "total_messages": {"$sum": 1}
            }},
            {"$sort": {"last_timestamp": -1}},
            {"$limit": limit}
        ]
        
        conversations_cursor = db.inbox_messages.aggregate(pipeline)
        conversations = await conversations_cursor.to_list(length=None)
        
        formatted_conversations = [
            {
                "sender_id": conv['_id']['sender_id'],
                "sender_name": conv['sender_name'],
                "sender_profile_pic": conv.get('sender_profile_pic'),
                "platform": conv['_id']['platform'],
                "last_message": conv['last_message'],
                "last_message_id": str(conv['last_message_id']),
                "last_timestamp": conv['last_timestamp'].isoformat() if conv['last_timestamp'] else None,
                "unread_count": conv['unread_count'],
                "total_messages": conv['total_messages']
            }
            for conv in conversations
        ]
        
        response = {
            "user_id": user_id,
            "platform_filter": platform,
            "conversations": formatted_conversations,
            "total_conversations": len(formatted_conversations)
        }
        
        await redis_service.set(cache_key, response, ttl=120)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching conversations: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch conversations: {str(e)}"
        )