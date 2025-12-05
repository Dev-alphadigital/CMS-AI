"""
Dashboard Aggregator - Aggregates data from all modules for dashboard view
"""

from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from app.core.database import get_database
from app.services.aggregators.ads_aggregator import AdsAggregator
from app.services.aggregators.seo_aggregator import SEOAggregator
from app.services.aggregators.inbox_aggregator import InboxAggregator
from app.services.aggregators.email_aggregator import EmailAggregator
from app.services.aggregators.branding_aggregator import BrandingAggregator
import logging

logger = logging.getLogger(__name__)


class DashboardAggregator:
    """
    Aggregates data from all modules for unified dashboard view
    """
    
    async def get_dashboard_overview(
        self,
        user_id: str,
        date_range: str = "last_30_days"
    ) -> Dict[str, Any]:
        """
        Get comprehensive dashboard overview combining all modules
        
        Args:
            user_id: User ID
            date_range: Date range string
            
        Returns:
            Combined dashboard data from all modules
        """
        # Calculate date range
        end = datetime.utcnow()
        if date_range == "last_7_days":
            start = end - timedelta(days=7)
        elif date_range == "last_30_days":
            start = end - timedelta(days=30)
        elif date_range == "last_90_days":
            start = end - timedelta(days=90)
        else:
            start = end - timedelta(days=30)
        
        # Aggregate data from all modules
        dashboard_data = {
            "user_id": user_id,
            "date_range": {
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "label": date_range
            },
            "modules": {}
        }
        
        try:
            # Ads data
            ads_aggregator = AdsAggregator()
            ads_data = await ads_aggregator.aggregate_all_platforms(
                user_id, start, end
            )
            dashboard_data["modules"]["ads"] = {
                "total_spend": ads_data.get("total_spend", 0),
                "total_clicks": ads_data.get("total_clicks", 0),
                "total_conversions": ads_data.get("total_conversions", 0),
                "platforms": len(ads_data.get("platform_breakdown", []))
            }
        except Exception as e:
            logger.error(f"Error aggregating ads data: {e}")
            dashboard_data["modules"]["ads"] = {"error": str(e)}
        
        try:
            # SEO data
            db = await get_database()
            seo_query = {
                "user_id": user_id,
                "date": {
                    "$gte": start.strftime("%Y-%m-%d"),
                    "$lte": end.strftime("%Y-%m-%d")
                }
            }
            seo_cursor = db.seo_metrics.find(seo_query).sort("date", -1).limit(1)
            seo_data = await seo_cursor.to_list(length=1)
            
            if seo_data:
                seo_aggregator = SEOAggregator()
                aggregated_seo = seo_aggregator.aggregate_seo_data(seo_data)
                dashboard_data["modules"]["seo"] = {
                    "total_organic_traffic": aggregated_seo.get("total_organic_traffic", 0),
                    "total_keywords": aggregated_seo.get("total_keywords", 0),
                    "avg_position": aggregated_seo.get("avg_position", 0)
                }
            else:
                dashboard_data["modules"]["seo"] = {"total_organic_traffic": 0}
        except Exception as e:
            logger.error(f"Error aggregating SEO data: {e}")
            dashboard_data["modules"]["seo"] = {"error": str(e)}
        
        try:
            # Inbox data
            inbox_aggregator = InboxAggregator()
            inbox_data = await inbox_aggregator.aggregate_all_platforms(
                user_id, start, end
            )
            dashboard_data["modules"]["inbox"] = {
                "total_messages": inbox_data.get("total_messages", 0),
                "unread_count": inbox_data.get("total_unread", 0),
                "reply_percentage": inbox_data.get("reply_percentage", 0)
            }
        except Exception as e:
            logger.error(f"Error aggregating inbox data: {e}")
            dashboard_data["modules"]["inbox"] = {"error": str(e)}
        
        try:
            # Email marketing data
            email_aggregator = EmailAggregator()
            email_data = await email_aggregator.aggregate_all_campaigns(
                user_id, start, end
            )
            dashboard_data["modules"]["email_marketing"] = {
                "total_campaigns": email_data.get("total_campaigns", 0),
                "total_sent": email_data.get("total_sent", 0),
                "overall_open_rate": email_data.get("overall_open_rate", 0)
            }
        except Exception as e:
            logger.error(f"Error aggregating email data: {e}")
            dashboard_data["modules"]["email_marketing"] = {"error": str(e)}
        
        try:
            # Branding data
            branding_aggregator = BrandingAggregator()
            branding_data = await branding_aggregator.aggregate_all_platforms(
                user_id, start, end
            )
            dashboard_data["modules"]["branding"] = {
                "total_followers": branding_data.get("total_followers", 0),
                "total_engagement": branding_data.get("total_engagement", 0),
                "avg_engagement_rate": branding_data.get("avg_engagement_rate", 0)
            }
        except Exception as e:
            logger.error(f"Error aggregating branding data: {e}")
            dashboard_data["modules"]["branding"] = {"error": str(e)}
        
        # Calculate summary metrics
        dashboard_data["summary"] = {
            "total_modules": len([m for m in dashboard_data["modules"].values() if "error" not in m]),
            "last_updated": datetime.utcnow().isoformat()
        }
        
        return dashboard_data


