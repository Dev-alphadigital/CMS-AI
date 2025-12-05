"""
Ads Analytics - Calculate metrics and trends for advertising data
"""

from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class AdsAnalytics:
    """
    Analytics engine for advertising metrics calculation
    """
    
    def calculate_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate key advertising metrics from aggregated data
        
        Args:
            data: Aggregated data containing spend, impressions, clicks, conversions
            
        Returns:
            Dict with calculated metrics including CTR, CPC, ROAS, conversion rate
        """
        total_spend = data.get("total_spend", 0) or data.get("spend", 0)
        total_impressions = data.get("total_impressions", 0) or data.get("impressions", 0)
        total_clicks = data.get("total_clicks", 0) or data.get("clicks", 0)
        total_conversions = data.get("total_conversions", 0) or data.get("conversions", 0)
        total_revenue = data.get("total_revenue", 0) or data.get("revenue", 0)
        
        # Calculate CTR (Click-Through Rate)
        avg_ctr = 0.0
        if total_impressions > 0:
            avg_ctr = round((total_clicks / total_impressions) * 100, 2)
        
        # Calculate CPC (Cost Per Click)
        avg_cpc = 0.0
        if total_clicks > 0:
            avg_cpc = round(total_spend / total_clicks, 2)
        
        # Calculate ROAS (Return on Ad Spend)
        avg_roas = 0.0
        if total_spend > 0:
            avg_roas = round(total_revenue / total_spend, 2)
        
        # Calculate Conversion Rate
        conversion_rate = 0.0
        if total_clicks > 0:
            conversion_rate = round((total_conversions / total_clicks) * 100, 2)
        
        # Calculate CPM (Cost Per Mille/Thousand Impressions)
        cpm = 0.0
        if total_impressions > 0:
            cpm = round((total_spend / total_impressions) * 1000, 2)
        
        # Calculate CPA (Cost Per Acquisition/Conversion)
        cpa = 0.0
        if total_conversions > 0:
            cpa = round(total_spend / total_conversions, 2)
        
        return {
            "total_spend": round(total_spend, 2),
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "total_conversions": total_conversions,
            "total_revenue": round(total_revenue, 2),
            "avg_ctr": avg_ctr,
            "avg_cpc": avg_cpc,
            "avg_roas": avg_roas,
            "conversion_rate": conversion_rate,
            "cpm": cpm,
            "cpa": cpa
        }
    
    def calculate_trends(
        self,
        current_metrics: Dict[str, Any],
        previous_metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate percentage change between current and previous period
        
        Args:
            current_metrics: Metrics for current period
            previous_metrics: Metrics for previous period
            
        Returns:
            Dict with percentage changes for key metrics
        """
        def calc_change(current: float, previous: float) -> float:
            """Calculate percentage change between two values"""
            if previous == 0:
                return 100.0 if current > 0 else 0.0
            return round(((current - previous) / previous) * 100, 2)
        
        return {
            "spend_change": calc_change(
                current_metrics.get("total_spend", 0),
                previous_metrics.get("total_spend", 0)
            ),
            "impressions_change": calc_change(
                current_metrics.get("total_impressions", 0),
                previous_metrics.get("total_impressions", 0)
            ),
            "clicks_change": calc_change(
                current_metrics.get("total_clicks", 0),
                previous_metrics.get("total_clicks", 0)
            ),
            "conversions_change": calc_change(
                current_metrics.get("total_conversions", 0),
                previous_metrics.get("total_conversions", 0)
            ),
            "ctr_change": calc_change(
                current_metrics.get("avg_ctr", 0),
                previous_metrics.get("avg_ctr", 0)
            ),
            "cpc_change": calc_change(
                current_metrics.get("avg_cpc", 0),
                previous_metrics.get("avg_cpc", 0)
            ),
            "roas_change": calc_change(
                current_metrics.get("avg_roas", 0),
                previous_metrics.get("avg_roas", 0)
            ),
            "conversion_rate_change": calc_change(
                current_metrics.get("conversion_rate", 0),
                previous_metrics.get("conversion_rate", 0)
            )
        }
    
    def calculate_platform_performance(
        self,
        platform_breakdown: list
    ) -> Dict[str, Any]:
        """
        Analyze performance across platforms
        
        Args:
            platform_breakdown: List of platform data dicts
            
        Returns:
            Analysis of best/worst performing platforms
        """
        if not platform_breakdown:
            return {
                "best_performing": None,
                "worst_performing": None,
                "recommendations": []
            }
        
        # Calculate efficiency score for each platform
        for platform in platform_breakdown:
            clicks = platform.get("clicks", 0)
            impressions = platform.get("impressions", 0)
            conversions = platform.get("conversions", 0)
            spend = platform.get("spend", 0)
            
            ctr = (clicks / impressions * 100) if impressions > 0 else 0
            conv_rate = (conversions / clicks * 100) if clicks > 0 else 0
            cpa = (spend / conversions) if conversions > 0 else float('inf')
            
            # Simple efficiency score (higher is better)
            platform["efficiency_score"] = round(
                (ctr * 0.3) + (conv_rate * 0.4) + (100 / max(cpa, 1) * 0.3),
                2
            )
        
        sorted_platforms = sorted(
            platform_breakdown,
            key=lambda x: x.get("efficiency_score", 0),
            reverse=True
        )
        
        return {
            "best_performing": sorted_platforms[0] if sorted_platforms else None,
            "worst_performing": sorted_platforms[-1] if len(sorted_platforms) > 1 else None,
            "ranking": [p["platform"] for p in sorted_platforms]
        }


