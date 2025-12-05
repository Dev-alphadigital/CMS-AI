"""
SEO Analytics - Calculate SEO metrics and trends
"""

from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class SEOAnalytics:
    """
    Analytics engine for SEO metrics calculation
    """
    
    def calculate_overview_metrics(self, aggregated: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate overview metrics from aggregated SEO data
        
        Args:
            aggregated: Aggregated SEO data from SEOAggregator
            
        Returns:
            Dict with calculated overview metrics
        """
        return {
            "total_organic_traffic": aggregated.get("total_organic_traffic", 0),
            "total_impressions": aggregated.get("total_impressions", 0),
            "total_clicks": aggregated.get("total_clicks", 0),
            "total_keywords": aggregated.get("total_keywords", 0),
            "top_10_keywords": aggregated.get("top_10_keywords", 0),
            "total_backlinks": aggregated.get("total_backlinks", 0),
            "domain_authority": aggregated.get("domain_authority", 0),
            "avg_position": aggregated.get("avg_position", 0.0),
            "avg_ctr": aggregated.get("avg_ctr", 0.0)
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
            "traffic_change": calc_change(
                current_metrics.get("total_organic_traffic", 0),
                previous_metrics.get("total_organic_traffic", 0)
            ),
            "impressions_change": calc_change(
                current_metrics.get("total_impressions", 0),
                previous_metrics.get("total_impressions", 0)
            ),
            "clicks_change": calc_change(
                current_metrics.get("total_clicks", 0),
                previous_metrics.get("total_clicks", 0)
            ),
            "ctr_change": calc_change(
                current_metrics.get("avg_ctr", 0),
                previous_metrics.get("avg_ctr", 0)
            ),
            "position_change": calc_change(
                current_metrics.get("avg_position", 0),
                previous_metrics.get("avg_position", 0)
            ),
            "keywords_change": calc_change(
                current_metrics.get("total_keywords", 0),
                previous_metrics.get("total_keywords", 0)
            ),
            "top_10_keywords_change": calc_change(
                current_metrics.get("top_10_keywords", 0),
                previous_metrics.get("top_10_keywords", 0)
            ),
            "backlinks_change": calc_change(
                current_metrics.get("total_backlinks", 0),
                previous_metrics.get("total_backlinks", 0)
            ),
            "domain_authority_change": calc_change(
                current_metrics.get("domain_authority", 0),
                previous_metrics.get("domain_authority", 0)
            )
        }
    
    def calculate_keyword_rankings(
        self,
        keywords_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze keyword rankings distribution
        
        Args:
            keywords_data: Dictionary of keyword metrics
            
        Returns:
            Analysis of keyword rankings
        """
        if not keywords_data:
            return {
                "top_3": 0,
                "top_10": 0,
                "top_20": 0,
                "top_50": 0,
                "top_100": 0,
                "beyond_100": 0
            }
        
        rankings = {
            "top_3": 0,
            "top_10": 0,
            "top_20": 0,
            "top_50": 0,
            "top_100": 0,
            "beyond_100": 0
        }
        
        for keyword, data in keywords_data.items():
            position = data.get("position", 0)
            
            if position <= 3:
                rankings["top_3"] += 1
            elif position <= 10:
                rankings["top_10"] += 1
            elif position <= 20:
                rankings["top_20"] += 1
            elif position <= 50:
                rankings["top_50"] += 1
            elif position <= 100:
                rankings["top_100"] += 1
            else:
                rankings["beyond_100"] += 1
        
        return rankings
    
    def calculate_page_performance(
        self,
        pages_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze page performance metrics
        
        Args:
            pages_data: Dictionary of page metrics
            
        Returns:
            Analysis of page performance
        """
        if not pages_data:
            return {
                "total_pages": 0,
                "high_performing": 0,
                "medium_performing": 0,
                "low_performing": 0,
                "avg_clicks_per_page": 0.0,
                "avg_ctr_per_page": 0.0
            }
        
        high_performing = 0
        medium_performing = 0
        low_performing = 0
        total_clicks = 0
        total_ctr = 0
        
        for page_url, data in pages_data.items():
            clicks = data.get("clicks", 0)
            ctr = data.get("ctr", 0)
            
            total_clicks += clicks
            total_ctr += ctr
            
            if clicks >= 100:
                high_performing += 1
            elif clicks >= 10:
                medium_performing += 1
            else:
                low_performing += 1
        
        total_pages = len(pages_data)
        avg_clicks_per_page = round(total_clicks / total_pages, 2) if total_pages > 0 else 0.0
        avg_ctr_per_page = round(total_ctr / total_pages, 2) if total_pages > 0 else 0.0
        
        return {
            "total_pages": total_pages,
            "high_performing": high_performing,
            "medium_performing": medium_performing,
            "low_performing": low_performing,
            "avg_clicks_per_page": avg_clicks_per_page,
            "avg_ctr_per_page": avg_ctr_per_page
        }


