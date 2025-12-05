"""
Prediction Engine - Time series forecasting for ads, SEO, etc.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import numpy as np

logger = logging.getLogger(__name__)


class PredictionEngine:
    """
    Time series prediction engine using statistical models
    """
    
    def __init__(self):
        self.min_data_points = 14  # Minimum days of data needed
    
    async def predict_ads_performance(
        self,
        historical_data: List[Dict[str, Any]],
        prediction_period: str = "next_7_days"
    ) -> Dict[str, Any]:
        """
        Predict future ad performance
        
        Args:
            historical_data: List of daily metrics
            prediction_period: 'next_7_days' or 'next_30_days'
            
        Returns:
            Predictions with confidence intervals
        """
        try:
            if len(historical_data) < self.min_data_points:
                raise ValueError(f"Need at least {self.min_data_points} days of data")
            
            # Extract time series
            dates = [datetime.fromisoformat(d['date']) for d in historical_data]
            spend = [d.get('spend', 0) for d in historical_data]
            impressions = [d.get('impressions', 0) for d in historical_data]
            clicks = [d.get('clicks', 0) for d in historical_data]
            conversions = [d.get('conversions', 0) for d in historical_data]
            
            # Determine forecast horizon
            forecast_days = 7 if prediction_period == "next_7_days" else 30
            
            # Generate predictions using simple moving average + trend
            predictions = {
                "spend": self._forecast_metric(spend, forecast_days),
                "impressions": self._forecast_metric(impressions, forecast_days),
                "clicks": self._forecast_metric(clicks, forecast_days),
                "conversions": self._forecast_metric(conversions, forecast_days)
            }
            
            # Calculate derived metrics
            predicted_ctr = (predictions["clicks"]["mean"] / predictions["impressions"]["mean"] * 100) \
                if predictions["impressions"]["mean"] > 0 else 0
            
            predicted_cpc = (predictions["spend"]["mean"] / predictions["clicks"]["mean"]) \
                if predictions["clicks"]["mean"] > 0 else 0
            
            # Calculate confidence based on data quality
            confidence = self._calculate_confidence(historical_data)
            
            return {
                "predictions": {
                    "spend": round(predictions["spend"]["mean"], 2),
                    "impressions": int(predictions["impressions"]["mean"]),
                    "clicks": int(predictions["clicks"]["mean"]),
                    "conversions": round(predictions["conversions"]["mean"], 2),
                    "ctr": round(predicted_ctr, 2),
                    "cpc": round(predicted_cpc, 2)
                },
                "confidence": confidence,
                "confidence_intervals": {
                    "spend": {
                        "lower": round(predictions["spend"]["lower"], 2),
                        "upper": round(predictions["spend"]["upper"], 2)
                    },
                    "clicks": {
                        "lower": int(predictions["clicks"]["lower"]),
                        "upper": int(predictions["clicks"]["upper"])
                    }
                },
                "model_info": {
                    "method": "Moving Average with Trend",
                    "data_points": len(historical_data),
                    "forecast_horizon": forecast_days
                }
            }
            
        except Exception as e:
            logger.error(f"Prediction failed: {str(e)}")
            raise
    
    async def predict_seo_growth(
        self,
        historical_data: List[Dict[str, Any]],
        prediction_period: str = "next_30_days"
    ) -> Dict[str, Any]:
        """
        Predict SEO metrics growth
        
        Args:
            historical_data: List of daily SEO metrics
            prediction_period: Forecast period
            
        Returns:
            Predictions for traffic, rankings, etc.
        """
        try:
            if len(historical_data) < self.min_data_points:
                raise ValueError(f"Need at least {self.min_data_points} days of data")
            
            # Extract metrics
            traffic = [d.get('organic_traffic', 0) for d in historical_data]
            keywords_count = [len(d.get('keywords', [])) for d in historical_data]
            
            # Average position across all keywords
            avg_positions = []
            for d in historical_data:
                keywords = d.get('keywords', [])
                if keywords:
                    positions = [k.get('position', 0) for k in keywords]
                    avg_positions.append(sum(positions) / len(positions))
                else:
                    avg_positions.append(0)
            
            forecast_days = 7 if prediction_period == "next_7_days" else 30
            
            # Generate predictions
            traffic_pred = self._forecast_metric(traffic, forecast_days)
            keywords_pred = self._forecast_metric(keywords_count, forecast_days)
            position_pred = self._forecast_metric(avg_positions, forecast_days, invert_trend=True)
            
            confidence = self._calculate_confidence(historical_data)
            
            # Generate recommendations
            recommendations = self._generate_seo_recommendations(
                historical_data, traffic_pred, position_pred
            )
            
            return {
                "predictions": {
                    "organic_traffic": int(traffic_pred["mean"]),
                    "keywords_count": int(keywords_pred["mean"]),
                    "avg_position": round(position_pred["mean"], 1)
                },
                "confidence": confidence,
                "recommendations": recommendations,
                "model_info": {
                    "method": "Trend Analysis",
                    "data_points": len(historical_data)
                }
            }
            
        except Exception as e:
            logger.error(f"SEO prediction failed: {str(e)}")
            raise
    
    async def forecast_seo_traffic(
        self,
        historical_data: List[Dict[str, Any]],
        forecast_days: int = 30
    ) -> Dict[str, Any]:
        """
        Forecast daily SEO traffic
        
        Returns:
            Daily predictions with confidence intervals
        """
        try:
            traffic = [d.get('organic_traffic', 0) for d in historical_data]
            dates = [datetime.fromisoformat(d['date']) for d in historical_data]
            
            # Generate daily forecasts
            daily_predictions = []
            base_date = dates[-1]
            
            for i in range(1, forecast_days + 1):
                forecast_date = base_date + timedelta(days=i)
                
                # Simple forecast using moving average + trend
                window = traffic[-7:]  # Last 7 days
                avg = sum(window) / len(window)
                
                # Calculate trend
                if len(traffic) >= 14:
                    recent_avg = sum(traffic[-7:]) / 7
                    older_avg = sum(traffic[-14:-7]) / 7
                    trend = (recent_avg - older_avg) / 7  # Daily trend
                else:
                    trend = 0
                
                # Apply trend
                prediction = avg + (trend * i)
                prediction = max(0, prediction)  # No negative traffic
                
                # Confidence interval (±20%)
                lower = prediction * 0.8
                upper = prediction * 1.2
                
                daily_predictions.append({
                    "date": forecast_date.strftime("%Y-%m-%d"),
                    "predicted_traffic": int(prediction),
                    "lower_bound": int(lower),
                    "upper_bound": int(upper)
                })
            
            total_predicted = sum(d["predicted_traffic"] for d in daily_predictions)
            
            # Determine trend
            if trend > 0:
                trend_label = "growing"
            elif trend < 0:
                trend_label = "declining"
            else:
                trend_label = "stable"
            
            return {
                "daily_predictions": daily_predictions,
                "expected_total_traffic": total_predicted,
                "confidence": self._calculate_confidence(historical_data),
                "trend": trend_label
            }
            
        except Exception as e:
            logger.error(f"Traffic forecast failed: {str(e)}")
            raise
    
    def _forecast_metric(
        self,
        values: List[float],
        forecast_days: int,
        invert_trend: bool = False
    ) -> Dict[str, float]:
        """
        Forecast a single metric using moving average + trend
        
        Args:
            values: Historical values
            forecast_days: Number of days to forecast
            invert_trend: If True, negative trend is good (e.g., position)
        """
        # Calculate moving average
        window_size = min(7, len(values))
        recent_values = values[-window_size:]
        mean_value = sum(recent_values) / len(recent_values)
        
        # Calculate trend
        if len(values) >= 14:
            recent_avg = sum(values[-7:]) / 7
            older_avg = sum(values[-14:-7]) / 7
            daily_trend = (recent_avg - older_avg) / 7
        else:
            daily_trend = 0
        
        # Apply trend
        if invert_trend:
            daily_trend = -abs(daily_trend)  # Always improve (lower position)
        
        forecasted_value = mean_value + (daily_trend * forecast_days)
        forecasted_value = max(0, forecasted_value)  # No negative values
        
        # Confidence interval (±25%)
        std_dev = np.std(recent_values) if len(recent_values) > 1 else mean_value * 0.25
        margin = std_dev * 1.5
        
        return {
            "mean": forecasted_value,
            "lower": max(0, forecasted_value - margin),
            "upper": forecasted_value + margin
        }
    
    def _calculate_confidence(self, data: List[Dict[str, Any]]) -> float:
        """
        Calculate prediction confidence based on data quality
        
        Returns:
            Confidence score (0.0 to 1.0)
        """
        # Base confidence on data quantity and consistency
        data_points = len(data)
        
        if data_points < 14:
            return 0.5
        elif data_points < 30:
            return 0.7
        elif data_points < 60:
            return 0.85
        else:
            return 0.95
    
    def _generate_seo_recommendations(
        self,
        historical_data: List[Dict[str, Any]],
        traffic_pred: Dict[str, float],
        position_pred: Dict[str, float]
    ) -> List[str]:
        """Generate SEO recommendations based on predictions"""
        recommendations = []
        
        # Analyze traffic trend
        if len(historical_data) >= 14:
            recent_traffic = sum(d.get('organic_traffic', 0) for d in historical_data[-7:])
            older_traffic = sum(d.get('organic_traffic', 0) for d in historical_data[-14:-7])
            
            if recent_traffic < older_traffic:
                recommendations.append(
                    "Traffic is declining. Review recent content changes and technical SEO issues."
                )
        
        # Analyze position
        if position_pred["mean"] > 10:
            recommendations.append(
                "Average position is low. Focus on improving content quality and building backlinks."
            )
        
        # Always add general recommendation
        if traffic_pred["mean"] > 0:
            recommendations.append(
                f"Expected traffic: {int(traffic_pred['mean'])} visits. "
                "Continue current SEO strategy."
            )
        
        return recommendations