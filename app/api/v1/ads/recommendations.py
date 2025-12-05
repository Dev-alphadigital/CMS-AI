"""
Ads Recommendations - AI-powered actionable recommendations
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.ai.recommendation_engine import RecommendationEngine
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/recommendations")
async def get_recommendations(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    priority: Optional[str] = Query(None, description="Filter by priority: high, medium, low"),
    status: str = Query("pending", description="pending, applied, dismissed"),
    limit: int = Query(10, ge=1, le=50)
) -> Dict[str, Any]:
    """
    Get AI-powered recommendations for ad optimization
    
    Returns:
    - Actionable recommendations
    - Priority levels
    - Expected impact
    - Implementation suggestions
    """
    try:
        cache_key = f"ads:recommendations:{user_id}:{platform}:{priority}:{status}:{limit}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Build query
        query_filter = {
            "user_id": user_id,
            "status": status
        }
        
        if platform:
            query_filter["platform"] = platform
        
        if priority:
            query_filter["priority"] = priority
        
        # Get recommendations from database
        recommendations_cursor = db.recommendations.find(query_filter).sort("generated_at", -1).limit(limit)
        recommendations = await recommendations_cursor.to_list(length=None)
        
        # If no recent recommendations, generate new ones
        if not recommendations:
            logger.info(f"No recommendations found, generating new ones for user: {user_id}")
            
            # Get recent performance data
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=30)
            
            campaigns_filter = {
                "user_id": user_id,
                "metrics.date": {
                    "$gte": start_date.strftime("%Y-%m-%d"),
                    "$lte": end_date.strftime("%Y-%m-%d")
                }
            }
            
            if platform:
                campaigns_filter["platform"] = platform
            
            campaigns_cursor = db.ad_campaigns.find(campaigns_filter)
            campaigns_data = await campaigns_cursor.to_list(length=None)
            
            if not campaigns_data:
                return {
                    "user_id": user_id,
                    "recommendations": [],
                    "total": 0,
                    "message": "No campaign data available for recommendations"
                }
            
            # Generate recommendations using AI
            recommendation_engine = RecommendationEngine()
            generated_recommendations = await recommendation_engine.generate_recommendations(
                campaigns_data=campaigns_data,
                user_id=user_id
            )
            
            # Store in database
            for rec in generated_recommendations:
                rec_doc = {
                    "user_id": user_id,
                    "platform": rec['platform'],
                    "campaign_id": rec.get('campaign_id'),
                    "recommendation": rec['recommendation'],
                    "priority": rec['priority'],
                    "expected_impact": rec.get('expected_impact'),
                    "action_items": rec.get('action_items', []),
                    "generated_at": datetime.utcnow(),
                    "status": "pending"
                }
                await db.recommendations.insert_one(rec_doc)
            
            recommendations = generated_recommendations
        
        # Format response
        formatted_recommendations = []
        for rec in recommendations:
            formatted_recommendations.append({
                "id": str(rec.get('_id', '')),
                "platform": rec.get('platform'),
                "campaign_id": rec.get('campaign_id'),
                "recommendation": rec['recommendation'],
                "priority": rec['priority'],
                "expected_impact": rec.get('expected_impact'),
                "action_items": rec.get('action_items', []),
                "generated_at": rec.get('generated_at', datetime.utcnow()).isoformat(),
                "status": rec.get('status', 'pending')
            })
        
        response = {
            "user_id": user_id,
            "filters": {
                "platform": platform,
                "priority": priority,
                "status": status
            },
            "recommendations": formatted_recommendations,
            "total": len(formatted_recommendations)
        }
        
        await redis_service.set(cache_key, response, ttl=3600)
        
        return response
        
    except Exception as e:
        logger.error(f"Error fetching recommendations: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch recommendations: {str(e)}"
        )


@router.get("/recommendations/campaign/{campaign_id}")
async def get_campaign_recommendations(
    user_id: str = Query(...),
    campaign_id: str = ...
) -> Dict[str, Any]:
    """
    Get recommendations specific to a campaign
    """
    try:
        db = await get_database()
        
        # Get campaign-specific recommendations
        query_filter = {
            "user_id": user_id,
            "campaign_id": campaign_id,
            "status": "pending"
        }
        
        recommendations_cursor = db.recommendations.find(query_filter).sort("priority", -1)
        recommendations = await recommendations_cursor.to_list(length=None)
        
        # If no recommendations exist, generate them
        if not recommendations:
            # Get campaign data
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=30)
            
            campaign_filter = {
                "user_id": user_id,
                "campaign_id": campaign_id,
                "metrics.date": {
                    "$gte": start_date.strftime("%Y-%m-%d"),
                    "$lte": end_date.strftime("%Y-%m-%d")
                }
            }
            
            campaign_cursor = db.ad_campaigns.find(campaign_filter)
            campaign_data = await campaign_cursor.to_list(length=None)
            
            if not campaign_data:
                raise HTTPException(
                    status_code=404,
                    detail="Campaign not found or no data available"
                )
            
            # Generate campaign-specific recommendations
            recommendation_engine = RecommendationEngine()
            generated_recs = await recommendation_engine.generate_campaign_recommendations(
                campaign_data=campaign_data,
                user_id=user_id
            )
            
            # Store in database
            for rec in generated_recs:
                rec_doc = {
                    "user_id": user_id,
                    "platform": campaign_data[0]['platform'],
                    "campaign_id": campaign_id,
                    "recommendation": rec['recommendation'],
                    "priority": rec['priority'],
                    "expected_impact": rec.get('expected_impact'),
                    "action_items": rec.get('action_items', []),
                    "generated_at": datetime.utcnow(),
                    "status": "pending"
                }
                await db.recommendations.insert_one(rec_doc)
            
            recommendations = generated_recs
        
        formatted_recommendations = [
            {
                "id": str(rec.get('_id', '')),
                "recommendation": rec['recommendation'],
                "priority": rec['priority'],
                "expected_impact": rec.get('expected_impact'),
                "action_items": rec.get('action_items', []),
                "generated_at": rec.get('generated_at', datetime.utcnow()).isoformat()
            }
            for rec in recommendations
        ]
        
        return {
            "user_id": user_id,
            "campaign_id": campaign_id,
            "recommendations": formatted_recommendations,
            "total": len(formatted_recommendations)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching campaign recommendations: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch campaign recommendations: {str(e)}"
        )


@router.patch("/recommendations/{recommendation_id}/status")
async def update_recommendation_status(
    recommendation_id: str,
    user_id: str = Query(...),
    new_status: str = Query(..., description="Status: applied, dismissed")
) -> Dict[str, Any]:
    """
    Update the status of a recommendation (mark as applied or dismissed)
    """
    try:
        from bson import ObjectId
        
        db = await get_database()
        
        # Update status
        result = await db.recommendations.update_one(
            {
                "_id": ObjectId(recommendation_id),
                "user_id": user_id
            },
            {
                "$set": {
                    "status": new_status,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        
        if result.modified_count == 0:
            raise HTTPException(
                status_code=404,
                detail="Recommendation not found"
            )
        
        # Invalidate cache
        await redis_service.delete_pattern(f"ads:recommendations:{user_id}:*")
        
        return {
            "recommendation_id": recommendation_id,
            "new_status": new_status,
            "updated_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating recommendation status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update recommendation status: {str(e)}"
        )


@router.post("/recommendations/generate")
async def trigger_recommendations_generation(
    user_id: str = Query(...),
    background_tasks: BackgroundTasks = None
) -> Dict[str, Any]:
    """
    Manually trigger recommendation generation
    """
    try:
        # TODO: Add background task
        # background_tasks.add_task(generate_recommendations_task, user_id)
        
        return {
            "status": "initiated",
            "user_id": user_id,
            "message": "Recommendation generation started",
            "initiated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error triggering recommendation generation: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger recommendation generation: {str(e)}"
        )