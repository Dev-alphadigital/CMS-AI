"""
Scheduled Posts - Content calendar and post scheduling
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


@router.get("/scheduled-posts")
async def get_scheduled_posts(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    status: str = Query("pending", description="Status: pending, published, failed, cancelled"),
    date_range: str = Query("upcoming", description="Date range: upcoming, this_week, this_month, all"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get list of scheduled posts
    
    Returns:
    - Scheduled posts with publish times
    - Platform and content details
    - Status and metadata
    """
    try:
        cache_key = f"branding:scheduled_posts:{user_id}:{platform}:{status}:{date_range}:{page}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Build query filter
        query_filter = {
            "user_id": user_id,
            "status": status
        }
        
        if platform:
            query_filter["platform"] = platform
        
        # Date range filter
        now = datetime.utcnow()
        
        if date_range == "upcoming":
            query_filter["scheduled_at"] = {"$gte": now}
        elif date_range == "this_week":
            week_end = now + timedelta(days=7)
            query_filter["scheduled_at"] = {"$gte": now, "$lte": week_end}
        elif date_range == "this_month":
            month_end = now + timedelta(days=30)
            query_filter["scheduled_at"] = {"$gte": now, "$lte": month_end}
        
        # Count total
        total_count = await db.scheduled_posts.count_documents(query_filter)
        
        # Calculate pagination
        skip = (page - 1) * limit
        
        # Fetch posts
        posts_cursor = db.scheduled_posts.find(query_filter).sort("scheduled_at", 1).skip(skip).limit(limit)
        posts = await posts_cursor.to_list(length=limit)
        
        # Format posts
        formatted_posts = []
        for post in posts:
            scheduled_at = post.get('scheduled_at')
            time_until = (scheduled_at - now).total_seconds() / 3600 if scheduled_at else 0
            
            formatted_posts.append({
                "id": str(post['_id']),
                "platform": post.get('platform'),
                "content": post.get('content'),
                "media": post.get('media', []),
                "hashtags": post.get('hashtags', []),
                "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
                "hours_until_publish": round(time_until, 2) if time_until > 0 else 0,
                "status": post.get('status'),
                "created_at": post.get('created_at').isoformat() if post.get('created_at') else None,
                "post_type": post.get('post_type')
            })
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        
        response = {
            "user_id": user_id,
            "filters": {
                "platform": platform,
                "status": status,
                "date_range": date_range
            },
            "scheduled_posts": formatted_posts,
            "pagination": {
                "page": page,
                "limit": limit,
                "total_posts": total_count,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
        
        # Cache for 5 minutes
        await redis_service.set(cache_key, response, ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching scheduled posts: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch scheduled posts: {str(e)}"
        )


@router.get("/scheduled-posts/{post_id}")
async def get_scheduled_post_details(
    user_id: str = Query(...),
    post_id: str = ...
) -> Dict[str, Any]:
    """
    Get details of a specific scheduled post
    """
    try:
        db = await get_database()
        
        post = await db.scheduled_posts.find_one({
            "_id": ObjectId(post_id),
            "user_id": user_id
        })
        
        if not post:
            raise HTTPException(
                status_code=404,
                detail="Scheduled post not found"
            )
        
        response = {
            "id": str(post['_id']),
            "platform": post.get('platform'),
            "content": post.get('content'),
            "media": post.get('media', []),
            "hashtags": post.get('hashtags', []),
            "post_type": post.get('post_type'),
            "scheduled_at": post.get('scheduled_at').isoformat() if post.get('scheduled_at') else None,
            "status": post.get('status'),
            "metadata": {
                "created_at": post.get('created_at').isoformat() if post.get('created_at') else None,
                "updated_at": post.get('updated_at').isoformat() if post.get('updated_at') else None,
                "published_at": post.get('published_at').isoformat() if post.get('published_at') else None,
                "error_message": post.get('error_message')
            }
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching scheduled post details: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch scheduled post details: {str(e)}"
        )


@router.post("/scheduled-posts")
async def create_scheduled_post(
    user_id: str = Query(...),
    platform: str = Body(..., description="Platform: facebook, instagram, twitter, linkedin"),
    content: str = Body(...),
    scheduled_at: str = Body(..., description="ISO datetime when to publish"),
    post_type: str = Body("text", description="Type: text, image, video, link"),
    media: Optional[List[str]] = Body(None, description="URLs of media files"),
    hashtags: Optional[List[str]] = Body(None),
    link_url: Optional[str] = Body(None)
) -> Dict[str, Any]:
    """
    Schedule a new post
    """
    try:
        db = await get_database()
        
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
        
        # Create post document
        post_doc = {
            "user_id": user_id,
            "platform": platform,
            "content": content,
            "post_type": post_type,
            "media": media or [],
            "hashtags": hashtags or [],
            "link_url": link_url,
            "scheduled_at": scheduled_datetime,
            "status": "pending",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        result = await db.scheduled_posts.insert_one(post_doc)
        
        # Invalidate cache
        await redis_service.delete_pattern(f"branding:scheduled_posts:{user_id}:*")
        
        logger.info(f"Post scheduled: {result.inserted_id} for {scheduled_datetime}")
        
        return {
            "status": "success",
            "post_id": str(result.inserted_id),
            "scheduled_at": scheduled_datetime.isoformat(),
            "message": "Post scheduled successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating scheduled post: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create scheduled post: {str(e)}"
        )


@router.patch("/scheduled-posts/{post_id}")
async def update_scheduled_post(
    user_id: str = Query(...),
    post_id: str = ...,
    content: Optional[str] = Body(None),
    scheduled_at: Optional[str] = Body(None),
    media: Optional[List[str]] = Body(None),
    hashtags: Optional[List[str]] = Body(None)
) -> Dict[str, Any]:
    """
    Update a scheduled post (only pending posts can be updated)
    """
    try:
        db = await get_database()
        
        # Check if post exists and is pending
        post = await db.scheduled_posts.find_one({
            "_id": ObjectId(post_id),
            "user_id": user_id
        })
        
        if not post:
            raise HTTPException(
                status_code=404,
                detail="Scheduled post not found"
            )
        
        if post.get('status') != 'pending':
            raise HTTPException(
                status_code=400,
                detail="Only pending posts can be updated"
            )
        
        # Build update document
        update_doc = {"updated_at": datetime.utcnow()}
        
        if content:
            update_doc["content"] = content
        
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
        
        if media is not None:
            update_doc["media"] = media
        
        if hashtags is not None:
            update_doc["hashtags"] = hashtags
        
        result = await db.scheduled_posts.update_one(
            {"_id": ObjectId(post_id)},
            {"$set": update_doc}
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"branding:scheduled_posts:{user_id}:*")
        
        return {
            "status": "success",
            "post_id": post_id,
            "message": "Post updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating scheduled post: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update scheduled post: {str(e)}"
        )


@router.delete("/scheduled-posts/{post_id}")
async def cancel_scheduled_post(
    user_id: str = Query(...),
    post_id: str = ...
) -> Dict[str, Any]:
    """
    Cancel a scheduled post
    """
    try:
        db = await get_database()
        
        # Check if post exists and is pending
        post = await db.scheduled_posts.find_one({
            "_id": ObjectId(post_id),
            "user_id": user_id
        })
        
        if not post:
            raise HTTPException(
                status_code=404,
                detail="Scheduled post not found"
            )
        
        if post.get('status') != 'pending':
            raise HTTPException(
                status_code=400,
                detail="Only pending posts can be cancelled"
            )
        
        # Update status to cancelled
        await db.scheduled_posts.update_one(
            {"_id": ObjectId(post_id)},
            {
                "$set": {
                    "status": "cancelled",
                    "cancelled_at": datetime.utcnow()
                }
            }
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"branding:scheduled_posts:{user_id}:*")
        
        logger.info(f"Post cancelled: {post_id}")
        
        return {
            "status": "success",
            "post_id": post_id,
            "message": "Post cancelled successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling scheduled post: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel scheduled post: {str(e)}"
        )


@router.get("/scheduled-posts/calendar")
async def get_content_calendar(
    user_id: str = Query(...),
    month: Optional[int] = Query(None, ge=1, le=12),
    year: Optional[int] = Query(None, ge=2020, le=2100)
) -> Dict[str, Any]:
    """
    Get content calendar view for a specific month
    """
    try:
        # Use current month if not specified
        now = datetime.utcnow()
        target_month = month or now.month
        target_year = year or now.year
        
        cache_key = f"branding:calendar:{user_id}:{target_month}:{target_year}"
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
            "status": "pending",
            "scheduled_at": {
                "$gte": start_date,
                "$lt": end_date
            }
        }
        
        # Fetch scheduled posts for this month
        posts_cursor = db.scheduled_posts.find(query_filter).sort("scheduled_at", 1)
        posts = await posts_cursor.to_list(length=None)
        
        # Group by date
        calendar_data = {}
        
        for post in posts:
            date_key = post['scheduled_at'].strftime("%Y-%m-%d")
            
            if date_key not in calendar_data:
                calendar_data[date_key] = []
            
            calendar_data[date_key].append({
                "post_id": str(post['_id']),
                "platform": post.get('platform'),
                "content_preview": post.get('content', '')[:100],
                "scheduled_time": post['scheduled_at'].strftime("%H:%M"),
                "post_type": post.get('post_type')
            })
        
        response = {
            "user_id": user_id,
            "month": target_month,
            "year": target_year,
            "calendar": calendar_data,
            "total_scheduled": len(posts)
        }
        
        # Cache for 10 minutes
        await redis_service.set(cache_key, response, ttl=600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching content calendar: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch content calendar: {str(e)}"
        )


@router.post("/scheduled-posts/{post_id}/publish-now")
async def publish_post_now(
    user_id: str = Query(...),
    post_id: str = ...
) -> Dict[str, Any]:
    """
    Publish a scheduled post immediately
    """
    try:
        db = await get_database()
        
        # Check if post exists
        post = await db.scheduled_posts.find_one({
            "_id": ObjectId(post_id),
            "user_id": user_id
        })
        
        if not post:
            raise HTTPException(
                status_code=404,
                detail="Scheduled post not found"
            )
        
        if post.get('status') != 'pending':
            raise HTTPException(
                status_code=400,
                detail="Only pending posts can be published"
            )
        
        # TODO: Integrate with social media platform APIs to actually publish
        
        # Update status
        await db.scheduled_posts.update_one(
            {"_id": ObjectId(post_id)},
            {
                "$set": {
                    "status": "published",
                    "published_at": datetime.utcnow()
                }
            }
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"branding:scheduled_posts:{user_id}:*")
        
        logger.info(f"Post published immediately: {post_id}")
        
        return {
            "status": "success",
            "post_id": post_id,
            "message": "Post published successfully",
            "published_at": datetime.utcnow().isoformat(),
            "note": "Platform API integration required for actual publishing"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error publishing post: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to publish post: {str(e)}"
        )