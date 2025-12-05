"""
Predictions API - AI-powered performance predictions
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.cache.redis_service import RedisService
from app.services.ai.prediction_engine import PredictionEngine
from app.schemas.predictions import PredictionResponse, PredictionRequest
import logging

router = APIRouter()
logger = logging.getLogger(__name__)
redis_service = RedisService()


@router.get("/predictions", response_model=PredictionResponse)
async def get_predictions(
    user_id: str = Query(...),
    platform: Optional[str] = Query(None),
    prediction_period: str = Query("next_7_days", description="next_7_days, next_30_days"),
    force_refresh: bool = Query(False, description="Force regenerate predictions")
) -> PredictionResponse:
    """
    Get AI predictions for performance metrics
    
    Returns:
    - Predicted metrics
    - Confidence intervals
    - Trend predictions
    - Risk factors
    """
    try:
        cache_key = f"predictions:{user_id}:{platform}:{prediction_period}"
        
        if not force_refresh:
            cached_data = await redis_service.get(cache_key)
            if cached_data:
                logger.info(f"Cache hit for predictions: {user_id}")
                return PredictionResponse(**cached_data)
        
        db = await get_database()
        
        # Check if predictions exist in database
        prediction_filter = {
            "user_id": user_id,
            "type": "performance",
            "prediction_period": prediction_period
        }
        
        if platform:
            prediction_filter["platform"] = platform
        
        # Check if we have recent predictions (generated in last 24 hours)
        recent_cutoff = datetime.utcnow() - timedelta(hours=24)
        prediction_filter["generated_at"] = {"$gte": recent_cutoff}
        
        existing_prediction = await db.predictions.find_one(prediction_filter)
        
        if existing_prediction and not force_refresh:
            response = PredictionResponse(
                user_id=user_id,
                platform=platform,
                prediction_period=prediction_period,
                predictions=existing_prediction['predicted_metrics'],
                confidence=existing_prediction.get('confidence', 0.85),
                generated_at=existing_prediction['generated_at'].isoformat(),
                source="database"
            )
            
            await redis_service.set(cache_key, response.dict(), ttl=86400)
            return response
        
        # No recent predictions - need to generate new ones
        logger.info(f"Generating new predictions for user: {user_id}")
        
        # Get historical data (last 90 days)
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        if platform:
            query_filter["platform"] = platform
        
        # Get historical metrics
        metrics_cursor = db.metrics.find(query_filter).sort("date", 1)
        historical_data = await metrics_cursor.to_list(length=None)
        
        if not historical_data or len(historical_data) < 14:
            raise HTTPException(
                status_code=400,
                detail="Insufficient historical data for predictions. Need at least 14 days of data."
            )
        
        # Generate predictions using AI
        prediction_engine = PredictionEngine()
        predictions = await prediction_engine.predict_performance(
            historical_data=historical_data,
            prediction_period=prediction_period
        )
        
        # Store predictions in database
        prediction_doc = {
            "user_id": user_id,
            "platform": platform,
            "type": "performance",
            "prediction_period": prediction_period,
            "predicted_metrics": predictions['predictions'],
            "confidence": predictions['confidence'],
            "model_info": predictions.get('model_info', {}),
            "generated_at": datetime.utcnow()
        }
        
        await db.predictions.insert_one(prediction_doc)
        
        response = PredictionResponse(
            user_id=user_id,
            platform=platform,
            prediction_period=prediction_period,
            predictions=predictions['predictions'],
            confidence=predictions['confidence'],
            model_info=predictions.get('model_info', {}),
            generated_at=datetime.utcnow().isoformat(),
            source="freshly_generated"
        )
        
        # Cache for 24 hours
        await redis_service.set(cache_key, response.dict(), ttl=86400)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating predictions: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate predictions: {str(e)}"
        )

