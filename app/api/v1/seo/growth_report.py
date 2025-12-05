"""
SEO Growth Report - AI-powered growth predictions and analysis
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


@router.get("/growth-report")
async def get_growth_report(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    prediction_period: str = Query("next_30_days", description="next_7_days, next_30_days, next_90_days")
) -> Dict[str, Any]:
    """
    Get comprehensive SEO growth report with AI predictions
    
    Returns:
    - Current growth trend
    - Predicted traffic for next period
    - Predicted keyword rankings
    - Growth opportunities
    - Risk factors
    - Actionable recommendations
    """
    try:
        cache_key = f"seo:growth_report:{user_id}:{domain}:{prediction_period}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            logger.info(f"Cache hit for growth report: {user_id}")
            return cached_data
        
        db = await get_database()
        
        # Get historical data (last 90 days for better predictions)
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        if domain:
            query_filter["domain"] = domain
        
        seo_cursor = db.seo_metrics.find(query_filter).sort("date", 1)
        historical_data = await seo_cursor.to_list(length=None)
        
        if not historical_data or len(historical_data) < 14:
            raise HTTPException(
                status_code=400,
                detail="Insufficient historical data for growth predictions. Need at least 14 days of data."
            )
        
        # Calculate current growth trend
        current_trend = _calculate_current_trend(historical_data)
        
        # Generate AI predictions
        prediction_engine = PredictionEngine()
        predictions = await prediction_engine.predict_seo_growth(
            historical_data=historical_data,
            prediction_period=prediction_period
        )
        
        # Identify growth opportunities
        opportunities = _identify_growth_opportunities(historical_data)
        
        # Identify risk factors
        risks = _identify_risk_factors(historical_data, predictions)
        
        # Store predictions in database
        prediction_doc = {
            "user_id": user_id,
            "domain": domain,
            "type": "seo_growth",
            "prediction_period": prediction_period,
            "predicted_metrics": predictions['predictions'],
            "confidence": predictions['confidence'],
            "generated_at": datetime.utcnow()
        }
        
        await db.predictions.insert_one(prediction_doc)
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "prediction_period": prediction_period,
            "current_trend": current_trend,
            "predictions": predictions['predictions'],
            "confidence": predictions['confidence'],
            "growth_opportunities": opportunities,
            "risk_factors": risks,
            "recommendations": predictions.get('recommendations', []),
            "generated_at": datetime.utcnow().isoformat()
        }
        
        # Cache for 24 hours
        await redis_service.set(cache_key, response, ttl=86400)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating growth report: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate growth report: {str(e)}"
        )


@router.get("/growth-report/monthly")
async def get_monthly_growth_analysis(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    months: int = Query(6, ge=3, le=24, description="Number of months to analyze")
) -> Dict[str, Any]:
    """
    Get month-over-month growth analysis
    
    Returns:
    - Monthly traffic growth
    - Monthly keyword growth
    - Monthly ranking improvements
    - Seasonal trends
    """
    try:
        cache_key = f"seo:monthly_growth:{user_id}:{domain}:{months}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Get data for specified months
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=months * 30)
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        if domain:
            query_filter["domain"] = domain
        
        seo_cursor = db.seo_metrics.find(query_filter).sort("date", 1)
        seo_data = await seo_cursor.to_list(length=None)
        
        if not seo_data:
            return {
                "user_id": user_id,
                "message": "No data available for monthly analysis"
            }
        
        # Group by month
        monthly_data = {}
        
        for doc in seo_data:
            date_obj = datetime.strptime(doc['date'], "%Y-%m-%d")
            month_key = date_obj.strftime("%Y-%m")
            
            if month_key not in monthly_data:
                monthly_data[month_key] = {
                    "traffic": 0,
                    "clicks": 0,
                    "impressions": 0,
                    "keywords_count": 0,
                    "top_10_keywords": 0
                }
            
            monthly_data[month_key]['traffic'] += doc.get('organic_traffic', 0)
            
            keywords = doc.get('keywords', [])
            monthly_data[month_key]['keywords_count'] = len(keywords)
            monthly_data[month_key]['top_10_keywords'] = sum(1 for kw in keywords if kw.get('position', 999) <= 10)
            
            for kw in keywords:
                monthly_data[month_key]['clicks'] += kw.get('clicks', 0)
                monthly_data[month_key]['impressions'] += kw.get('impressions', 0)
        
        # Calculate month-over-month growth
        monthly_summary = []
        sorted_months = sorted(monthly_data.keys())
        
        for i, month in enumerate(sorted_months):
            data = monthly_data[month]
            
            growth_rate = None
            if i > 0:
                previous_month = sorted_months[i-1]
                previous_traffic = monthly_data[previous_month]['traffic']
                current_traffic = data['traffic']
                
                if previous_traffic > 0:
                    growth_rate = ((current_traffic - previous_traffic) / previous_traffic * 100)
            
            monthly_summary.append({
                "month": month,
                "traffic": data['traffic'],
                "clicks": data['clicks'],
                "impressions": data['impressions'],
                "keywords_count": data['keywords_count'],
                "top_10_keywords": data['top_10_keywords'],
                "growth_rate": round(growth_rate, 2) if growth_rate is not None else None
            })
        
        # Calculate average monthly growth
        growth_rates = [m['growth_rate'] for m in monthly_summary if m['growth_rate'] is not None]
        avg_monthly_growth = sum(growth_rates) / len(growth_rates) if growth_rates else 0
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "months_analyzed": len(monthly_summary),
            "average_monthly_growth": round(avg_monthly_growth, 2),
            "monthly_data": monthly_summary,
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        }
        
        await redis_service.set(cache_key, response, ttl=86400)
        
        return response
        
    except Exception as e:
        logger.error(f"Error generating monthly growth analysis: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate monthly growth analysis: {str(e)}"
        )


@router.get("/growth-report/forecast")
async def get_traffic_forecast(
    user_id: str = Query(...),
    domain: Optional[str] = Query(None),
    forecast_days: int = Query(30, ge=7, le=90)
) -> Dict[str, Any]:
    """
    Get daily traffic forecast using AI
    
    Returns:
    - Daily predicted traffic
    - Confidence intervals (upper/lower bounds)
    - Expected keyword ranking changes
    """
    try:
        cache_key = f"seo:traffic_forecast:{user_id}:{domain}:{forecast_days}"
        cached_data = await redis_service.get(cache_key)
        
        if cached_data:
            return cached_data
        
        db = await get_database()
        
        # Get historical data (90 days)
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        if domain:
            query_filter["domain"] = domain
        
        seo_cursor = db.seo_metrics.find(query_filter).sort("date", 1)
        historical_data = await seo_cursor.to_list(length=None)
        
        if not historical_data or len(historical_data) < 30:
            raise HTTPException(
                status_code=400,
                detail="Insufficient historical data for forecast. Need at least 30 days."
            )
        
        # Generate forecast
        prediction_engine = PredictionEngine()
        forecast = await prediction_engine.forecast_seo_traffic(
            historical_data=historical_data,
            forecast_days=forecast_days
        )
        
        response = {
            "user_id": user_id,
            "domain": domain,
            "forecast_days": forecast_days,
            "daily_forecast": forecast['daily_predictions'],
            "summary": {
                "expected_total_traffic": forecast['expected_total_traffic'],
                "confidence": forecast['confidence'],
                "trend": forecast['trend']
            },
            "generated_at": datetime.utcnow().isoformat()
        }
        
        await redis_service.set(cache_key, response, ttl=43200)  # 12 hours
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating traffic forecast: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate traffic forecast: {str(e)}"
        )


@router.post("/growth-report/generate")
async def trigger_growth_report_generation(
    user_id: str = Query(...),
    background_tasks: BackgroundTasks = None
) -> Dict[str, Any]:
    """
    Manually trigger growth report generation (background task)
    """
    try:
        # TODO: Add background task
        # background_tasks.add_task(generate_growth_report_task, user_id)
        
        return {
            "status": "initiated",
            "user_id": user_id,
            "message": "Growth report generation started",
            "initiated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error triggering growth report generation: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger growth report generation: {str(e)}"
        )


def _calculate_current_trend(historical_data: List[Dict]) -> Dict[str, Any]:
    """Calculate current growth trend from historical data"""
    if len(historical_data) < 2:
        return {"trend": "insufficient_data"}
    
    # Compare last 7 days vs previous 7 days
    recent_data = historical_data[-7:]
    previous_data = historical_data[-14:-7]
    
    recent_traffic = sum(doc.get('organic_traffic', 0) for doc in recent_data)
    previous_traffic = sum(doc.get('organic_traffic', 0) for doc in previous_data)
    
    if previous_traffic > 0:
        growth_rate = ((recent_traffic - previous_traffic) / previous_traffic * 100)
    else:
        growth_rate = 0
    
    # Determine trend direction
    if growth_rate > 10:
        trend = "strong_growth"
    elif growth_rate > 2:
        trend = "moderate_growth"
    elif growth_rate > -2:
        trend = "stable"
    elif growth_rate > -10:
        trend = "slight_decline"
    else:
        trend = "significant_decline"
    
    return {
        "trend": trend,
        "growth_rate": round(growth_rate, 2),
        "recent_traffic": recent_traffic,
        "previous_traffic": previous_traffic
    }


def _identify_growth_opportunities(historical_data: List[Dict]) -> List[Dict]:
    """Identify growth opportunities from historical data"""
    opportunities = []
    
    if not historical_data:
        return opportunities
    
    latest_data = historical_data[-1]
    keywords = latest_data.get('keywords', [])
    
    # Opportunity 1: High volume keywords in positions 11-20
    quick_wins = [
        kw for kw in keywords
        if 11 <= kw.get('position', 999) <= 20 and kw.get('volume', 0) >= 500
    ]
    
    if quick_wins:
        opportunities.append({
            "type": "quick_wins",
            "title": "Quick Win Keywords",
            "description": f"{len(quick_wins)} high-volume keywords ranking 11-20 that could be pushed to page 1",
            "priority": "high",
            "potential_impact": "high"
        })
    
    # Opportunity 2: Growing keywords (improving positions)
    if len(historical_data) >= 30:
        month_ago = historical_data[-30]
        month_ago_positions = {kw['keyword']: kw['position'] for kw in month_ago.get('keywords', [])}
        
        improving_keywords = [
            kw for kw in keywords
            if kw['keyword'] in month_ago_positions and
            month_ago_positions[kw['keyword']] - kw.get('position', 999) >= 5
        ]
        
        if improving_keywords:
            opportunities.append({
                "type": "momentum",
                "title": "Keywords with Positive Momentum",
                "description": f"{len(improving_keywords)} keywords showing consistent improvement",
                "priority": "medium",
                "potential_impact": "medium"
            })
    
    # Opportunity 3: Low CTR keywords with good positions
    low_ctr_top_keywords = [
        kw for kw in keywords
        if kw.get('position', 999) <= 10 and kw.get('ctr', 0) < 5
    ]
    
    if low_ctr_top_keywords:
        opportunities.append({
            "type": "optimization",
            "title": "CTR Optimization Opportunity",
            "description": f"{len(low_ctr_top_keywords)} top-ranking keywords with low CTR need meta description optimization",
            "priority": "medium",
            "potential_impact": "medium"
        })
    
    return opportunities


def _identify_risk_factors(historical_data: List[Dict], predictions: Dict) -> List[Dict]:
    """Identify potential risk factors"""
    risks = []
    
    if not historical_data:
        return risks
    
    # Risk 1: Declining traffic trend
    if len(historical_data) >= 14:
        recent_avg = sum(doc.get('organic_traffic', 0) for doc in historical_data[-7:]) / 7
        previous_avg = sum(doc.get('organic_traffic', 0) for doc in historical_data[-14:-7]) / 7
        
        if previous_avg > 0 and recent_avg < previous_avg * 0.9:  # 10% decline
            risks.append({
                "type": "traffic_decline",
                "title": "Traffic Decline Detected",
                "description": f"Traffic has declined by {round((1 - recent_avg/previous_avg) * 100, 1)}% in the last 7 days",
                "severity": "high",
                "action_required": "Investigate ranking losses and technical issues"
            })
    
    # Risk 2: Keywords losing positions
    if len(historical_data) >= 30:
        current_keywords = historical_data[-1].get('keywords', [])
        month_ago_keywords = historical_data[-30].get('keywords', [])
        
        month_ago_positions = {kw['keyword']: kw['position'] for kw in month_ago_keywords}
        
        declining_keywords = [
            kw for kw in current_keywords
            if kw['keyword'] in month_ago_positions and
            kw.get('position', 999) - month_ago_positions[kw['keyword']] >= 5
        ]
        
        if len(declining_keywords) >= 10:
            risks.append({
                "type": "ranking_loss",
                "title": "Multiple Keywords Losing Rankings",
                "description": f"{len(declining_keywords)} keywords have dropped 5+ positions in the last 30 days",
                "severity": "high",
                "action_required": "Review content quality and backlink profile"
            })
    
    # Risk 3: Low prediction confidence
    if predictions.get('confidence', 1.0) < 0.7:
        risks.append({
            "type": "unpredictable_performance",
            "title": "Unpredictable Performance Pattern",
            "description": "SEO performance shows irregular patterns making predictions less reliable",
            "severity": "medium",
            "action_required": "Review recent changes and algorithm updates"
        })
    
    return risks