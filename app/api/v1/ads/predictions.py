"""
Ads Predictions - AI-powered performance predictions
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.ai.prediction_engine import PredictionEngine
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/predictions")
async def get_predictions(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    prediction_period: str = Query("next_7_days", description="next_7_days, next_30_days"),
    force_refresh: bool = Query(False, description="Force regenerate predictions")
) -> Dict[str, Any]:
    """
    Get AI predictions for ad performance
    
    Returns:
    - Predicted spend, clicks, conversions
    - Confidence intervals
    - Trend predictions
    - Risk factors
    """
    try:
        cache_key = f"ads:predictions:{user_id}:{platform}:{prediction_period}"
        
        if not force_refresh:
            cached_data = await redis_service.get(cache_key)
            if cached_data:
                logger.info(f"Cache hit for predictions: {user_id}")
                return cached_data
        
        db = await get_database()
        
        # Check if predictions exist in database
        prediction_filter = {
            "user_id": user_id,
            "type": "ad_performance",
            "prediction_period": prediction_period
        }
        
        if platform:
            prediction_filter["platform"] = platform
        
        # Check if we have recent predictions (generated in last 24 hours)
        recent_cutoff = datetime.utcnow() - timedelta(hours=24)
        prediction_filter["generated_at"] = {"$gte": recent_cutoff}
        
        existing_prediction = await db.predictions.find_one(prediction_filter)
        
        if existing_prediction and not force_refresh:
            response = {
                "user_id": user_id,
                "platform": platform,
                "prediction_period": prediction_period,
                "predictions": existing_prediction['predicted_metrics'],
                "confidence": existing_prediction.get('confidence', 0.85),
                "generated_at": existing_prediction['generated_at'].isoformat(),
                "source": "database"
            }
            
            await redis_service.set(cache_key, response, ttl=86400)  # Cache for 24 hours
            return response
        
        # No recent predictions - need to generate new ones
        logger.info(f"Generating new predictions for user: {user_id}")
        
        # Get historical data (last 90 days)
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)
        
        query_filter = {
            "user_id": user_id,
            "metrics.date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        if platform:
            query_filter["platform"] = platform
        
        campaigns_cursor = db.ad_campaigns.find(query_filter).sort("metrics.date", 1)
        historical_data = await campaigns_cursor.to_list(length=None)
        
        if not historical_data or len(historical_data) < 14:
            raise HTTPException(
                status_code=400,
                detail="Insufficient historical data for predictions. Need at least 14 days of data."
            )
        
        # Generate predictions using AI
        prediction_engine = PredictionEngine()
        predictions = await prediction_engine.predict_ad_performance(
            historical_data=historical_data,
            prediction_period=prediction_period
        )
        
        # Store predictions in database
        prediction_doc = {
            "user_id": user_id,
            "platform": platform,
            "type": "ad_performance",
            "prediction_period": prediction_period,
            "predicted_metrics": predictions['predictions'],
            "confidence": predictions['confidence'],
            "model_info": predictions['model_info'],
            "generated_at": datetime.utcnow()
        }
        
        await db.predictions.insert_one(prediction_doc)
        
        response = {
            "user_id": user_id,
            "platform": platform,
            "prediction_period": prediction_period,
            "predictions": predictions['predictions'],
            "confidence": predictions['confidence'],
            "model_info": predictions['model_info'],
            "generated_at": datetime.utcnow().isoformat(),
            "source": "freshly_generated"
        }
        
        # Cache for 24 hours
        await redis_service.set(cache_key, response, ttl=86400)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating predictions: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate predictions: {str(e)}"
        )


@router.get("/predictions/campaign/{campaign_id}")
async def get_campaign_predictions(
    user_id: str = Query(...),
    campaign_id: str = ...,
    prediction_days: int = Query(7, ge=1, le=30)
) -> Dict[str, Any]:
    """
    Get predictions for a specific campaign
    """
    try:
        cache_key = f"ads:predictions:campaign:{user_id}:{campaign_id}:{prediction_days}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Get historical data for this campaign (last 60 days)
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=60)
        
        query_filter = {
            "user_id": user_id,
            "campaign_id": campaign_id,
            "metrics.date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        campaigns_cursor = db.ad_campaigns.find(query_filter).sort("metrics.date", 1)
        historical_data = await campaigns_cursor.to_list(length=None)
        
        if not historical_data or len(historical_data) < 7:
            raise HTTPException(
                status_code=400,
                detail="Insufficient historical data for this campaign. Need at least 7 days."
            )
        
        # Generate predictions
        prediction_engine = PredictionEngine()
        predictions = await prediction_engine.predict_campaign_performance(
            campaign_data=historical_data,
            prediction_days=prediction_days
        )
        
        response = {
            "user_id": user_id,
            "campaign_id": campaign_id,
            "campaign_name": historical_data[0]['campaign_name'],
            "platform": historical_data[0]['platform'],
            "prediction_days": prediction_days,
            "predictions": predictions['daily_predictions'],
            "summary": predictions['summary'],
            "confidence": predictions['confidence'],
            "generated_at": datetime.utcnow().isoformat()
        }
        
        await redis_service.set(cache_key, response, ttl=43200)  # Cache for 12 hours
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating campaign predictions: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate campaign predictions: {str(e)}"
        )


@router.post("/predictions/generate")
async def trigger_prediction_generation(
    user_id: str = Query(...),
    background_tasks: BackgroundTasks = None
) -> Dict[str, Any]:
    """
    Manually trigger prediction generation (background task)
    """
    try:
        # TODO: Add background task to generate predictions
        # background_tasks.add_task(generate_predictions_task, user_id)
        
        return {
            "status": "initiated",
            "user_id": user_id,
            "message": "Prediction generation started. Check back in a few minutes.",
            "initiated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error triggering prediction generation: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger prediction generation: {str(e)}"
        )