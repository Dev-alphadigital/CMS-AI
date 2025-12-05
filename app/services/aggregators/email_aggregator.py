"""
Email Aggregator - Aggregates email marketing data across campaigns
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from app.core.database import get_database
import logging

logger = logging.getLogger(__name__)


class EmailAggregator:
    """
    Aggregates email marketing campaign data and metrics
    """
    
    async def aggregate_all_campaigns(
        self,
        user_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Aggregate email campaign data across all campaigns
        
        Args:
            user_id: User ID
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Aggregated email marketing statistics
        """
        db = await get_database()
        
        # Build query filter
        query_filter = {"user_id": user_id, "status": "sent"}
        
        if start_date or end_date:
            query_filter["sent_at"] = {}
            if start_date:
                query_filter["sent_at"]["$gte"] = start_date
            if end_date:
                query_filter["sent_at"]["$lte"] = end_date
        
        # Aggregate campaign metrics
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": None,
                "total_campaigns": {"$sum": 1},
                "total_sent": {"$sum": "$sent"},
                "total_opened": {"$sum": "$opened"},
                "total_clicked": {"$sum": "$clicked"},
                "total_bounced": {"$sum": "$bounced"},
                "total_unsubscribed": {"$sum": "$unsubscribed"},
                "avg_open_rate": {"$avg": "$open_rate"},
                "avg_click_rate": {"$avg": "$click_rate"},
                "avg_bounce_rate": {"$avg": "$bounce_rate"}
            }}
        ]
        
        result = await db.email_campaigns.aggregate(pipeline).to_list(length=1)
        
        if result:
            stats = result[0]
            total_sent = stats.get("total_sent", 0)
            
            return {
                "total_campaigns": stats.get("total_campaigns", 0),
                "total_sent": total_sent,
                "total_opened": stats.get("total_opened", 0),
                "total_clicked": stats.get("total_clicked", 0),
                "total_bounced": stats.get("total_bounced", 0),
                "total_unsubscribed": stats.get("total_unsubscribed", 0),
                "overall_open_rate": round(
                    (stats.get("total_opened", 0) / total_sent * 100) if total_sent > 0 else 0,
                    2
                ),
                "overall_click_rate": round(
                    (stats.get("total_clicked", 0) / total_sent * 100) if total_sent > 0 else 0,
                    2
                ),
                "overall_bounce_rate": round(
                    (stats.get("total_bounced", 0) / total_sent * 100) if total_sent > 0 else 0,
                    2
                ),
                "avg_open_rate": round(stats.get("avg_open_rate", 0), 2),
                "avg_click_rate": round(stats.get("avg_click_rate", 0), 2),
                "avg_bounce_rate": round(stats.get("avg_bounce_rate", 0), 2)
            }
        
        return {
            "total_campaigns": 0,
            "total_sent": 0,
            "total_opened": 0,
            "total_clicked": 0,
            "total_bounced": 0,
            "total_unsubscribed": 0,
            "overall_open_rate": 0.0,
            "overall_click_rate": 0.0,
            "overall_bounce_rate": 0.0,
            "avg_open_rate": 0.0,
            "avg_click_rate": 0.0,
            "avg_bounce_rate": 0.0
        }
    
    async def aggregate_by_campaign_type(
        self,
        user_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Aggregate campaigns by type (newsletter, promotional, transactional, automated)
        
        Args:
            user_id: User ID
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Breakdown by campaign type
        """
        db = await get_database()
        
        query_filter = {"user_id": user_id, "status": "sent"}
        
        if start_date or end_date:
            query_filter["sent_at"] = {}
            if start_date:
                query_filter["sent_at"]["$gte"] = start_date
            if end_date:
                query_filter["sent_at"]["$lte"] = end_date
        
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": "$campaign_type",
                "campaign_count": {"$sum": 1},
                "total_sent": {"$sum": "$sent"},
                "total_opened": {"$sum": "$opened"},
                "total_clicked": {"$sum": "$clicked"},
                "total_bounced": {"$sum": "$bounced"},
                "avg_open_rate": {"$avg": "$open_rate"},
                "avg_click_rate": {"$avg": "$click_rate"}
            }},
            {"$sort": {"total_sent": -1}}
        ]
        
        type_breakdown = await db.email_campaigns.aggregate(pipeline).to_list(length=None)
        
        result = {}
        for item in type_breakdown:
            campaign_type = item["_id"]
            total_sent = item.get("total_sent", 0)
            
            result[campaign_type] = {
                "campaign_count": item.get("campaign_count", 0),
                "total_sent": total_sent,
                "total_opened": item.get("total_opened", 0),
                "total_clicked": item.get("total_clicked", 0),
                "total_bounced": item.get("total_bounced", 0),
                "open_rate": round(
                    (item.get("total_opened", 0) / total_sent * 100) if total_sent > 0 else 0,
                    2
                ),
                "click_rate": round(
                    (item.get("total_clicked", 0) / total_sent * 100) if total_sent > 0 else 0,
                    2
                ),
                "avg_open_rate": round(item.get("avg_open_rate", 0), 2),
                "avg_click_rate": round(item.get("avg_click_rate", 0), 2)
            }
        
        return result
    
    async def get_campaign_performance_over_time(
        self,
        user_id: str,
        start_date: datetime,
        end_date: datetime,
        group_by: str = "day"
    ) -> List[Dict[str, Any]]:
        """
        Get campaign performance metrics over time
        
        Args:
            user_id: User ID
            start_date: Start date
            end_date: End date
            group_by: Group by "day", "week", or "month"
            
        Returns:
            List of performance metrics by time period
        """
        db = await get_database()
        
        query_filter = {
            "user_id": user_id,
            "status": "sent",
            "sent_at": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        
        # Determine date format based on group_by
        if group_by == "day":
            date_format = "%Y-%m-%d"
        elif group_by == "week":
            date_format = "%Y-W%U"
        elif group_by == "month":
            date_format = "%Y-%m"
        else:
            date_format = "%Y-%m-%d"
        
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": {
                    "$dateToString": {
                        "format": date_format,
                        "date": "$sent_at"
                    }
                },
                "campaign_count": {"$sum": 1},
                "total_sent": {"$sum": "$sent"},
                "total_opened": {"$sum": "$opened"},
                "total_clicked": {"$sum": "$clicked"},
                "total_bounced": {"$sum": "$bounced"}
            }},
            {"$sort": {"_id": 1}}
        ]
        
        performance_data = await db.email_campaigns.aggregate(pipeline).to_list(length=None)
        
        result = []
        for item in performance_data:
            total_sent = item.get("total_sent", 0)
            result.append({
                "period": item["_id"],
                "campaign_count": item.get("campaign_count", 0),
                "total_sent": total_sent,
                "total_opened": item.get("total_opened", 0),
                "total_clicked": item.get("total_clicked", 0),
                "total_bounced": item.get("total_bounced", 0),
                "open_rate": round(
                    (item.get("total_opened", 0) / total_sent * 100) if total_sent > 0 else 0,
                    2
                ),
                "click_rate": round(
                    (item.get("total_clicked", 0) / total_sent * 100) if total_sent > 0 else 0,
                    2
                )
            })
        
        return result
    
    async def get_top_performing_campaigns(
        self,
        user_id: str,
        metric: str = "open_rate",
        limit: int = 10,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get top performing campaigns by metric
        
        Args:
            user_id: User ID
            metric: Metric to sort by (open_rate, click_rate, click_to_open_rate)
            limit: Number of campaigns to return
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of top performing campaigns
        """
        db = await get_database()
        
        query_filter = {
            "user_id": user_id,
            "status": "sent"
        }
        
        if start_date or end_date:
            query_filter["sent_at"] = {}
            if start_date:
                query_filter["sent_at"]["$gte"] = start_date
            if end_date:
                query_filter["sent_at"]["$lte"] = end_date
        
        # Calculate click_to_open_rate if needed
        if metric == "click_to_open_rate":
            # Need to fetch and calculate manually
            campaigns_cursor = db.email_campaigns.find(query_filter)
            campaigns = await campaigns_cursor.to_list(length=None)
            
            for campaign in campaigns:
                opened = campaign.get("opened", 0)
                clicked = campaign.get("clicked", 0)
                if opened > 0:
                    campaign["click_to_open_rate"] = round((clicked / opened) * 100, 2)
                else:
                    campaign["click_to_open_rate"] = 0.0
            
            top_campaigns = sorted(
                campaigns,
                key=lambda x: x.get("click_to_open_rate", 0),
                reverse=True
            )[:limit]
        else:
            campaigns_cursor = db.email_campaigns.find(query_filter).sort(metric, -1).limit(limit)
            top_campaigns = await campaigns_cursor.to_list(length=limit)
        
        result = []
        for campaign in top_campaigns:
            result.append({
                "campaign_id": str(campaign["_id"]),
                "campaign_name": campaign.get("campaign_name"),
                "subject": campaign.get("subject"),
                "campaign_type": campaign.get("campaign_type"),
                "sent": campaign.get("sent", 0),
                "opened": campaign.get("opened", 0),
                "clicked": campaign.get("clicked", 0),
                "open_rate": campaign.get("open_rate", 0),
                "click_rate": campaign.get("click_rate", 0),
                "click_to_open_rate": campaign.get("click_to_open_rate", 0),
                "sent_at": campaign.get("sent_at")
            })
        
        return result
    
    async def get_engagement_trends(
        self,
        user_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Calculate engagement trends comparing two periods
        
        Args:
            user_id: User ID
            start_date: Start date of current period
            end_date: End date of current period
            
        Returns:
            Comparison of current vs previous period
        """
        # Get current period data
        current_data = await self.aggregate_all_campaigns(user_id, start_date, end_date)
        
        # Calculate previous period (same length)
        period_length = (end_date - start_date).days
        previous_end = start_date
        previous_start = previous_end - timedelta(days=period_length)
        
        # Get previous period data
        previous_data = await self.aggregate_all_campaigns(user_id, previous_start, previous_end)
        
        def calc_change(current: float, previous: float) -> float:
            """Calculate percentage change"""
            if previous == 0:
                return 100.0 if current > 0 else 0.0
            return round(((current - previous) / previous) * 100, 2)
        
        return {
            "current_period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "metrics": current_data
            },
            "previous_period": {
                "start_date": previous_start.isoformat(),
                "end_date": previous_end.isoformat(),
                "metrics": previous_data
            },
            "trends": {
                "campaigns_change": calc_change(
                    current_data.get("total_campaigns", 0),
                    previous_data.get("total_campaigns", 0)
                ),
                "sent_change": calc_change(
                    current_data.get("total_sent", 0),
                    previous_data.get("total_sent", 0)
                ),
                "open_rate_change": calc_change(
                    current_data.get("overall_open_rate", 0),
                    previous_data.get("overall_open_rate", 0)
                ),
                "click_rate_change": calc_change(
                    current_data.get("overall_click_rate", 0),
                    previous_data.get("overall_click_rate", 0)
                )
            }
        }


