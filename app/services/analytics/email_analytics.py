"""
Email Analytics - Calculate email marketing metrics and insights
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class EmailAnalytics:
    """
    Analytics engine for email marketing metrics calculation
    """
    
    def calculate_overview_metrics(self, campaign_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate overview metrics from campaign data
        
        Args:
            campaign_data: List of campaign documents
            
        Returns:
            Dict with calculated overview metrics
        """
        if not campaign_data:
            return {
                "total_campaigns": 0,
                "total_sent": 0,
                "total_opened": 0,
                "total_clicked": 0,
                "total_bounced": 0,
                "total_unsubscribed": 0,
                "avg_open_rate": 0.0,
                "avg_click_rate": 0.0,
                "avg_bounce_rate": 0.0,
                "overall_open_rate": 0.0,
                "overall_click_rate": 0.0,
                "overall_bounce_rate": 0.0
            }
        
        total_campaigns = len(campaign_data)
        total_sent = sum(c.get("sent", 0) for c in campaign_data)
        total_opened = sum(c.get("opened", 0) for c in campaign_data)
        total_clicked = sum(c.get("clicked", 0) for c in campaign_data)
        total_bounced = sum(c.get("bounced", 0) for c in campaign_data)
        total_unsubscribed = sum(c.get("unsubscribed", 0) for c in campaign_data)
        
        # Calculate average rates
        open_rates = [c.get("open_rate", 0) for c in campaign_data if c.get("open_rate") is not None]
        click_rates = [c.get("click_rate", 0) for c in campaign_data if c.get("click_rate") is not None]
        bounce_rates = [c.get("bounce_rate", 0) for c in campaign_data if c.get("bounce_rate") is not None]
        
        avg_open_rate = sum(open_rates) / len(open_rates) if open_rates else 0.0
        avg_click_rate = sum(click_rates) / len(click_rates) if click_rates else 0.0
        avg_bounce_rate = sum(bounce_rates) / len(bounce_rates) if bounce_rates else 0.0
        
        # Calculate overall rates
        overall_open_rate = (total_opened / total_sent * 100) if total_sent > 0 else 0.0
        overall_click_rate = (total_clicked / total_sent * 100) if total_sent > 0 else 0.0
        overall_bounce_rate = (total_bounced / total_sent * 100) if total_sent > 0 else 0.0
        
        return {
            "total_campaigns": total_campaigns,
            "total_sent": total_sent,
            "total_opened": total_opened,
            "total_clicked": total_clicked,
            "total_bounced": total_bounced,
            "total_unsubscribed": total_unsubscribed,
            "avg_open_rate": round(avg_open_rate, 2),
            "avg_click_rate": round(avg_click_rate, 2),
            "avg_bounce_rate": round(avg_bounce_rate, 2),
            "overall_open_rate": round(overall_open_rate, 2),
            "overall_click_rate": round(overall_click_rate, 2),
            "overall_bounce_rate": round(overall_bounce_rate, 2)
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
            "campaigns_change": calc_change(
                current_metrics.get("total_campaigns", 0),
                previous_metrics.get("total_campaigns", 0)
            ),
            "sent_change": calc_change(
                current_metrics.get("total_sent", 0),
                previous_metrics.get("total_sent", 0)
            ),
            "opened_change": calc_change(
                current_metrics.get("total_opened", 0),
                previous_metrics.get("total_opened", 0)
            ),
            "clicked_change": calc_change(
                current_metrics.get("total_clicked", 0),
                previous_metrics.get("total_clicked", 0)
            ),
            "open_rate_change": calc_change(
                current_metrics.get("overall_open_rate", 0),
                previous_metrics.get("overall_open_rate", 0)
            ),
            "click_rate_change": calc_change(
                current_metrics.get("overall_click_rate", 0),
                previous_metrics.get("overall_click_rate", 0)
            ),
            "bounce_rate_change": calc_change(
                current_metrics.get("overall_bounce_rate", 0),
                previous_metrics.get("overall_bounce_rate", 0)
            )
        }
    
    def analyze_subject_lines(
        self,
        campaign_data: List[Dict[str, Any]],
        min_sends: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Analyze subject line performance
        
        Args:
            campaign_data: List of campaign documents
            min_sends: Minimum sends to include in analysis
            
        Returns:
            List of subject lines sorted by performance
        """
        subject_performance = []
        
        for campaign in campaign_data:
            sent = campaign.get("sent", 0)
            if sent >= min_sends:
                subject_performance.append({
                    "subject": campaign.get("subject", "No Subject"),
                    "open_rate": campaign.get("open_rate", 0),
                    "click_rate": campaign.get("click_rate", 0),
                    "click_to_open_rate": round(
                        (campaign.get("clicked", 0) / campaign.get("opened", 1) * 100)
                        if campaign.get("opened", 0) > 0 else 0,
                        2
                    ),
                    "sent": sent,
                    "opened": campaign.get("opened", 0),
                    "clicked": campaign.get("clicked", 0)
                })
        
        # Sort by open rate
        return sorted(subject_performance, key=lambda x: x["open_rate"], reverse=True)
    
    def analyze_send_times(
        self,
        campaign_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Analyze best send times based on campaign performance
        
        Args:
            campaign_data: List of campaign documents with sent_at timestamps
            
        Returns:
            List of hourly performance data
        """
        hourly_performance = {}
        
        for campaign in campaign_data:
            sent_at = campaign.get("sent_at")
            if not sent_at:
                continue
            
            # Handle both datetime objects and ISO strings
            if isinstance(sent_at, str):
                try:
                    sent_at = datetime.fromisoformat(sent_at.replace('Z', '+00:00'))
                except:
                    continue
            
            if not isinstance(sent_at, datetime):
                continue
            
            hour = sent_at.hour
            
            if hour not in hourly_performance:
                hourly_performance[hour] = {
                    "hour": hour,
                    "campaigns": 0,
                    "total_open_rate": 0.0,
                    "total_click_rate": 0.0,
                    "total_sent": 0
                }
            
            hourly_performance[hour]["campaigns"] += 1
            hourly_performance[hour]["total_open_rate"] += campaign.get("open_rate", 0)
            hourly_performance[hour]["total_click_rate"] += campaign.get("click_rate", 0)
            hourly_performance[hour]["total_sent"] += campaign.get("sent", 0)
        
        # Calculate averages
        result = []
        for hour, data in hourly_performance.items():
            count = data["campaigns"]
            result.append({
                "hour": hour,
                "campaigns": count,
                "avg_open_rate": round(data["total_open_rate"] / count, 2) if count > 0 else 0.0,
                "avg_click_rate": round(data["total_click_rate"] / count, 2) if count > 0 else 0.0,
                "total_sent": data["total_sent"]
            })
        
        # Sort by average open rate
        return sorted(result, key=lambda x: x["avg_open_rate"], reverse=True)
    
    def calculate_engagement_score(
        self,
        campaign: Dict[str, Any]
    ) -> float:
        """
        Calculate overall engagement score for a campaign
        
        Args:
            campaign: Campaign document
            
        Returns:
            Engagement score (0-100)
        """
        open_rate = campaign.get("open_rate", 0)
        click_rate = campaign.get("click_rate", 0)
        bounce_rate = campaign.get("bounce_rate", 0)
        unsubscribe_rate = campaign.get("unsubscribe_rate", 0)
        
        # Weighted score: opens (40%), clicks (40%), low bounces (10%), low unsubscribes (10%)
        score = (
            (open_rate * 0.4) +
            (click_rate * 10 * 0.4) +  # Multiply click rate by 10 to normalize
            ((100 - bounce_rate * 10) * 0.1) +  # Lower bounce is better
            ((100 - unsubscribe_rate * 10) * 0.1)  # Lower unsubscribe is better
        )
        
        return round(min(max(score, 0), 100), 2)
    
    def compare_to_benchmark(
        self,
        user_metrics: Dict[str, Any],
        benchmark: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compare user metrics to industry benchmarks
        
        Args:
            user_metrics: User's email metrics
            benchmark: Industry benchmark metrics
            
        Returns:
            Comparison analysis
        """
        open_rate_diff = user_metrics.get("avg_open_rate", 0) - benchmark.get("avg_open_rate", 0)
        click_rate_diff = user_metrics.get("avg_click_rate", 0) - benchmark.get("avg_click_rate", 0)
        bounce_rate_diff = user_metrics.get("avg_bounce_rate", 0) - benchmark.get("avg_bounce_rate", 0)
        
        # Determine overall performance
        if open_rate_diff > 0 and click_rate_diff > 0:
            performance_summary = "above_average"
        elif open_rate_diff < 0 and click_rate_diff < 0:
            performance_summary = "below_average"
        else:
            performance_summary = "mixed"
        
        return {
            "open_rate_diff": round(open_rate_diff, 2),
            "click_rate_diff": round(click_rate_diff, 2),
            "bounce_rate_diff": round(bounce_rate_diff, 2),
            "performance_summary": performance_summary,
            "percentile_estimate": self._estimate_percentile(open_rate_diff, click_rate_diff)
        }
    
    def _estimate_percentile(
        self,
        open_rate_diff: float,
        click_rate_diff: float
    ) -> str:
        """
        Estimate percentile ranking based on differences from benchmark
        """
        avg_diff = (open_rate_diff + click_rate_diff) / 2
        
        if avg_diff > 5:
            return "top_10"
        elif avg_diff > 2:
            return "top_25"
        elif avg_diff > 0:
            return "top_50"
        elif avg_diff > -2:
            return "bottom_50"
        elif avg_diff > -5:
            return "bottom_25"
        else:
            return "bottom_10"
    
    def calculate_roi(
        self,
        campaign: Dict[str, Any],
        revenue_per_click: float = 0.0
    ) -> Dict[str, Any]:
        """
        Calculate ROI for a campaign
        
        Args:
            campaign: Campaign document
            revenue_per_click: Average revenue per click
            
        Returns:
            ROI metrics
        """
        sent = campaign.get("sent", 0)
        clicked = campaign.get("clicked", 0)
        
        # Estimate cost (assuming $0.001 per email sent)
        cost_per_email = 0.001
        total_cost = sent * cost_per_email
        
        # Calculate revenue
        total_revenue = clicked * revenue_per_click
        
        # Calculate ROI
        roi = ((total_revenue - total_cost) / total_cost * 100) if total_cost > 0 else 0.0
        
        return {
            "total_cost": round(total_cost, 2),
            "total_revenue": round(total_revenue, 2),
            "roi_percentage": round(roi, 2),
            "revenue_per_email": round(total_revenue / sent, 4) if sent > 0 else 0.0,
            "cost_per_click": round(total_cost / clicked, 4) if clicked > 0 else 0.0
        }


