"""
Email Marketing Campaigns - Campaign management and overview
"""

from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.aggregators.email_aggregator import EmailAggregator
from bson import ObjectId
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/campaigns")
async def get_campaigns(
    user_id: str = Query(...),
    status: Optional[str] = Query(None, description="Filter by status: draft, scheduled, sent, archived"),
    campaign_type: Optional[str] = Query(None, description="Filter by type: newsletter, promotional, transactional, automated"),
    date_range: str = Query("last_30_days", description="Date range: last_7_days, last_30_days, last_90_days, all_time"),
    sort_by: str = Query("sent_at", description="Sort by: sent_at, name, open_rate, click_rate"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get list of email campaigns with filters and sorting
    
    Returns:
    - Campaign details
    - Performance metrics (opens, clicks, bounces)
    - Status and timestamps
    """
    try:
        cache_key = f"email:campaigns:{user_id}:{status}:{campaign_type}:{date_range}:{sort_by}:{page}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            logger.info(f"Cache hit for email campaigns: {user_id}")
            return cached_data
        
        db = await get_database()
        
        # Build query filter
        query_filter = {"user_id": user_id}
        
        if status:
            query_filter["status"] = status
        
        if campaign_type:
            query_filter["campaign_type"] = campaign_type
        
        # Date range filter
        if date_range != "all_time":
            dates = _calculate_date_range(date_range)
            query_filter["sent_at"] = {
                "$gte": dates['start_date'],
                "$lte": dates['end_date']
            }
        
        # Count total campaigns
        total_count = await db.email_campaigns.count_documents(query_filter)
        
        # Calculate pagination
        skip = (page - 1) * limit
        
        # Fetch campaigns
        sort_direction = -1 if order == "desc" else 1
        campaigns_cursor = db.email_campaigns.find(query_filter).sort(sort_by, sort_direction).skip(skip).limit(limit)
        campaigns = await campaigns_cursor.to_list(length=limit)
        
        # Format campaigns
        formatted_campaigns = []
        for campaign in campaigns:
            formatted_campaigns.append({
                "id": str(campaign['_id']),
                "campaign_name": campaign.get('campaign_name'),
                "campaign_type": campaign.get('campaign_type'),
                "subject": campaign.get('subject'),
                "status": campaign.get('status'),
                "sent": campaign.get('sent', 0),
                "opened": campaign.get('opened', 0),
                "clicked": campaign.get('clicked', 0),
                "bounced": campaign.get('bounced', 0),
                "unsubscribed": campaign.get('unsubscribed', 0),
                "open_rate": campaign.get('open_rate', 0),
                "click_rate": campaign.get('click_rate', 0),
                "bounce_rate": campaign.get('bounce_rate', 0),
                "sent_at": campaign.get('sent_at').isoformat() if campaign.get('sent_at') else None,
                "created_at": campaign.get('created_at').isoformat() if campaign.get('created_at') else None
            })
        
        # Calculate pagination info
        total_pages = (total_count + limit - 1) // limit
        
        response = {
            "user_id": user_id,
            "filters": {
                "status": status,
                "campaign_type": campaign_type,
                "date_range": date_range,
                "sort_by": sort_by,
                "order": order
            },
            "campaigns": formatted_campaigns,
            "pagination": {
                "page": page,
                "limit": limit,
                "total_campaigns": total_count,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1
            }
        }
        
        # Cache for 5 minutes
        await redis_service.set(cache_key, response, ttl=300)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching email campaigns: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch email campaigns: {str(e)}"
        )


@router.get("/campaigns/{campaign_id}")
async def get_campaign_details(
    user_id: str = Query(...),
    campaign_id: str = ...
) -> Dict[str, Any]:
    """
    Get detailed information about a specific campaign
    
    Returns:
    - Full campaign details
    - Performance metrics
    - Engagement over time
    - Link click tracking
    - Device/client breakdown
    """
    try:
        cache_key = f"email:campaign_details:{user_id}:{campaign_id}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Fetch campaign
        campaign = await db.email_campaigns.find_one({
            "_id": ObjectId(campaign_id),
            "user_id": user_id
        })
        
        if not campaign:
            raise HTTPException(
                status_code=404,
                detail="Campaign not found"
            )
        
        # Calculate detailed metrics
        sent = campaign.get('sent', 0)
        opened = campaign.get('opened', 0)
        clicked = campaign.get('clicked', 0)
        bounced = campaign.get('bounced', 0)
        unsubscribed = campaign.get('unsubscribed', 0)
        
        open_rate = (opened / sent * 100) if sent > 0 else 0
        click_rate = (clicked / sent * 100) if sent > 0 else 0
        click_to_open_rate = (clicked / opened * 100) if opened > 0 else 0
        bounce_rate = (bounced / sent * 100) if sent > 0 else 0
        unsubscribe_rate = (unsubscribed / sent * 100) if sent > 0 else 0
        
        response = {
            "id": str(campaign['_id']),
            "campaign_name": campaign.get('campaign_name'),
            "campaign_type": campaign.get('campaign_type'),
            "subject": campaign.get('subject'),
            "preview_text": campaign.get('preview_text'),
            "from_name": campaign.get('from_name'),
            "from_email": campaign.get('from_email'),
            "reply_to": campaign.get('reply_to'),
            "status": campaign.get('status'),
            "metrics": {
                "sent": sent,
                "opened": opened,
                "clicked": clicked,
                "bounced": bounced,
                "unsubscribed": unsubscribed,
                "open_rate": round(open_rate, 2),
                "click_rate": round(click_rate, 2),
                "click_to_open_rate": round(click_to_open_rate, 2),
                "bounce_rate": round(bounce_rate, 2),
                "unsubscribe_rate": round(unsubscribe_rate, 2)
            },
            "engagement_over_time": campaign.get('engagement_over_time', []),
            "link_clicks": campaign.get('link_clicks', []),
            "device_breakdown": campaign.get('device_breakdown', {}),
            "email_client_breakdown": campaign.get('email_client_breakdown', {}),
            "geographic_data": campaign.get('geographic_data', []),
            "sent_at": campaign.get('sent_at').isoformat() if campaign.get('sent_at') else None,
            "created_at": campaign.get('created_at').isoformat() if campaign.get('created_at') else None,
            "updated_at": campaign.get('updated_at').isoformat() if campaign.get('updated_at') else None
        }
        
        # Cache for 10 minutes
        await redis_service.set(cache_key, response, ttl=600)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching campaign details: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch campaign details: {str(e)}"
        )


@router.post("/campaigns")
async def create_campaign(
    user_id: str = Query(...),
    campaign_name: str = Body(...),
    campaign_type: str = Body(..., description="newsletter, promotional, transactional, automated"),
    subject: str = Body(...),
    preview_text: Optional[str] = Body(None),
    from_name: str = Body(...),
    from_email: str = Body(...),
    reply_to: Optional[str] = Body(None),
    html_content: Optional[str] = Body(None),
    plain_text_content: Optional[str] = Body(None),
    recipient_list_id: Optional[str] = Body(None),
    tags: Optional[List[str]] = Body(None)
) -> Dict[str, Any]:
    """
    Create a new email campaign (draft)
    """
    try:
        db = await get_database()
        
        campaign_doc = {
            "user_id": user_id,
            "campaign_name": campaign_name,
            "campaign_type": campaign_type,
            "subject": subject,
            "preview_text": preview_text,
            "from_name": from_name,
            "from_email": from_email,
            "reply_to": reply_to or from_email,
            "html_content": html_content,
            "plain_text_content": plain_text_content,
            "recipient_list_id": recipient_list_id,
            "tags": tags or [],
            "status": "draft",
            "sent": 0,
            "opened": 0,
            "clicked": 0,
            "bounced": 0,
            "unsubscribed": 0,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        result = await db.email_campaigns.insert_one(campaign_doc)
        
        # Invalidate cache
        await redis_service.delete_pattern(f"email:campaigns:{user_id}:*")
        
        logger.info(f"Email campaign created: {result.inserted_id} for user {user_id}")
        
        return {
            "status": "success",
            "campaign_id": str(result.inserted_id),
            "campaign_name": campaign_name,
            "message": "Campaign created successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating campaign: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create campaign: {str(e)}"
        )


@router.patch("/campaigns/{campaign_id}")
async def update_campaign(
    user_id: str = Query(...),
    campaign_id: str = ...,
    campaign_name: Optional[str] = Body(None),
    subject: Optional[str] = Body(None),
    preview_text: Optional[str] = Body(None),
    html_content: Optional[str] = Body(None),
    plain_text_content: Optional[str] = Body(None),
    tags: Optional[List[str]] = Body(None)
) -> Dict[str, Any]:
    """
    Update campaign details (only for draft campaigns)
    """
    try:
        db = await get_database()
        
        # Check if campaign is in draft status
        campaign = await db.email_campaigns.find_one({
            "_id": ObjectId(campaign_id),
            "user_id": user_id
        })
        
        if not campaign:
            raise HTTPException(
                status_code=404,
                detail="Campaign not found"
            )
        
        if campaign.get('status') != 'draft':
            raise HTTPException(
                status_code=400,
                detail="Only draft campaigns can be edited"
            )
        
        # Build update document
        update_doc = {"updated_at": datetime.utcnow()}
        
        if campaign_name:
            update_doc["campaign_name"] = campaign_name
        if subject:
            update_doc["subject"] = subject
        if preview_text is not None:
            update_doc["preview_text"] = preview_text
        if html_content is not None:
            update_doc["html_content"] = html_content
        if plain_text_content is not None:
            update_doc["plain_text_content"] = plain_text_content
        if tags is not None:
            update_doc["tags"] = tags
        
        result = await db.email_campaigns.update_one(
            {"_id": ObjectId(campaign_id)},
            {"$set": update_doc}
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"email:campaign_details:{user_id}:{campaign_id}")
        await redis_service.delete_pattern(f"email:campaigns:{user_id}:*")
        
        return {
            "status": "success",
            "campaign_id": campaign_id,
            "message": "Campaign updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating campaign: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update campaign: {str(e)}"
        )


@router.delete("/campaigns/{campaign_id}")
async def delete_campaign(
    user_id: str = Query(...),
    campaign_id: str = ...
) -> Dict[str, Any]:
    """
    Delete a campaign (only drafts can be deleted)
    """
    try:
        db = await get_database()
        
        # Check if campaign is draft
        campaign = await db.email_campaigns.find_one({
            "_id": ObjectId(campaign_id),
            "user_id": user_id
        })
        
        if not campaign:
            raise HTTPException(
                status_code=404,
                detail="Campaign not found"
            )
        
        if campaign.get('status') != 'draft':
            raise HTTPException(
                status_code=400,
                detail="Only draft campaigns can be deleted. Use archive for sent campaigns."
            )
        
        result = await db.email_campaigns.delete_one({
            "_id": ObjectId(campaign_id),
            "user_id": user_id
        })
        
        # Invalidate cache
        await redis_service.delete_pattern(f"email:campaigns:{user_id}:*")
        
        return {
            "status": "success",
            "campaign_id": campaign_id,
            "message": "Campaign deleted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting campaign: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete campaign: {str(e)}"
        )


@router.post("/campaigns/{campaign_id}/send")
async def send_campaign(
    user_id: str = Query(...),
    campaign_id: str = ...,
    send_immediately: bool = Body(True, embed=True)
) -> Dict[str, Any]:
    """
    Send a campaign (or schedule for later)
    
    Note: This requires integration with email service provider (SendGrid, Mailgun, etc.)
    """
    try:
        db = await get_database()
        
        # Fetch campaign
        campaign = await db.email_campaigns.find_one({
            "_id": ObjectId(campaign_id),
            "user_id": user_id
        })
        
        if not campaign:
            raise HTTPException(
                status_code=404,
                detail="Campaign not found"
            )
        
        if campaign.get('status') not in ['draft', 'scheduled']:
            raise HTTPException(
                status_code=400,
                detail="Campaign has already been sent"
            )
        
        # Validate campaign has required fields
        if not campaign.get('subject') or not campaign.get('html_content'):
            raise HTTPException(
                status_code=400,
                detail="Campaign must have subject and content before sending"
            )
        
        # TODO: Integrate with email service provider (SendGrid, Mailgun, AWS SES)
        # For now, just update status
        
        update_doc = {
            "status": "sent",
            "sent_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        # TODO: Get actual recipient count from ESP
        # update_doc["sent"] = recipient_count
        
        await db.email_campaigns.update_one(
            {"_id": ObjectId(campaign_id)},
            {"$set": update_doc}
        )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"email:campaign_details:{user_id}:{campaign_id}")
        await redis_service.delete_pattern(f"email:campaigns:{user_id}:*")
        
        logger.info(f"Campaign {campaign_id} sent for user {user_id}")
        
        return {
            "status": "success",
            "campaign_id": campaign_id,
            "message": "Campaign sent successfully",
            "sent_at": datetime.utcnow().isoformat(),
            "note": "Email service provider integration required for actual sending"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending campaign: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send campaign: {str(e)}"
        )


@router.patch("/campaigns/{campaign_id}/archive")
async def archive_campaign(
    user_id: str = Query(...),
    campaign_id: str = ...
) -> Dict[str, Any]:
    """
    Archive a campaign
    """
    try:
        db = await get_database()
        
        result = await db.email_campaigns.update_one(
            {"_id": ObjectId(campaign_id), "user_id": user_id},
            {"$set": {"status": "archived", "archived_at": datetime.utcnow()}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Campaign not found"
            )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"email:campaigns:{user_id}:*")
        
        return {
            "status": "success",
            "campaign_id": campaign_id,
            "message": "Campaign archived successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error archiving campaign: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to archive campaign: {str(e)}"
        )


@router.get("/campaigns/top-performers")
async def get_top_performing_campaigns(
    user_id: str = Query(...),
    metric: str = Query("open_rate", description="Metric: open_rate, click_rate, click_to_open_rate"),
    limit: int = Query(10, ge=1, le=50)
) -> Dict[str, Any]:
    """
    Get top performing campaigns based on specific metric
    """
    try:
        cache_key = f"email:top_performers:{user_id}:{metric}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Only include sent campaigns
        query_filter = {
            "user_id": user_id,
            "status": "sent"
        }
        
        # Sort by metric
        campaigns_cursor = db.email_campaigns.find(query_filter).sort(metric, -1).limit(limit)
        campaigns = await campaigns_cursor.to_list(length=limit)
        
        formatted_campaigns = [
            {
                "id": str(c['_id']),
                "campaign_name": c.get('campaign_name'),
                "subject": c.get('subject'),
                "metric_value": c.get(metric, 0),
                "sent": c.get('sent', 0),
                "opened": c.get('opened', 0),
                "clicked": c.get('clicked', 0),
                "sent_at": c.get('sent_at').isoformat() if c.get('sent_at') else None
            }
            for c in campaigns
        ]
        
        response = {
            "user_id": user_id,
            "metric": metric,
            "top_campaigns": formatted_campaigns
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching top performing campaigns: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch top performing campaigns: {str(e)}"
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