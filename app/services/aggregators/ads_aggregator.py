"""
Ads Aggregator - Aggregates ad data across all platforms
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from app.core.database import get_database
import logging

logger = logging.getLogger(__name__)


class AdsAggregator:
    """
    Aggregates advertising data from multiple platforms:
    - Meta Ads (Facebook/Instagram)
    - Google Ads
    - Twitter Ads
    - TikTok Ads
    - LinkedIn Ads
    """
    
    SUPPORTED_PLATFORMS = [
        "meta_ads",
        "google_ads", 
        "twitter_ads",
        "tiktok_ads",
        "linkedin_ads"
    ]
    
    async def aggregate_all_platforms(
        self,
        user_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Aggregate ad data from all connected platforms for a user
        
        Returns:
            Dict containing aggregated metrics, platform breakdown, and top campaigns
        """
        db = await get_database()
        
        platform_breakdown = []
        all_campaigns = []
        
        total_spend = 0.0
        total_impressions = 0
        total_clicks = 0
        total_conversions = 0
        
        for platform in self.SUPPORTED_PLATFORMS:
            try:
                platform_data = await self._get_platform_data(
                    db, user_id, platform, start_date, end_date
                )
                
                if platform_data:
                    platform_breakdown.append({
                        "platform": platform,
                        "spend": platform_data.get("spend", 0),
                        "impressions": platform_data.get("impressions", 0),
                        "clicks": platform_data.get("clicks", 0),
                        "conversions": platform_data.get("conversions", 0)
                    })
                    
                    total_spend += platform_data.get("spend", 0)
                    total_impressions += platform_data.get("impressions", 0)
                    total_clicks += platform_data.get("clicks", 0)
                    total_conversions += platform_data.get("conversions", 0)
                    
                    all_campaigns.extend(platform_data.get("campaigns", []))
                    
            except Exception as e:
                logger.warning(f"Failed to get data for platform {platform}: {e}")
                continue
        
        # Sort campaigns by performance (ROAS or conversions)
        top_campaigns = sorted(
            all_campaigns,
            key=lambda x: x.get("roas", 0),
            reverse=True
        )
        
        return {
            "total_spend": total_spend,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "total_conversions": total_conversions,
            "platform_breakdown": platform_breakdown,
            "top_campaigns": top_campaigns
        }
    
    async def aggregate_single_platform(
        self,
        user_id: str,
        platform: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Get aggregated data for a single platform
        """
        if platform not in self.SUPPORTED_PLATFORMS:
            raise ValueError(f"Unsupported platform: {platform}")
        
        db = await get_database()
        
        platform_data = await self._get_platform_data(
            db, user_id, platform, start_date, end_date
        )
        
        daily_data = await self._get_daily_breakdown(
            db, user_id, platform, start_date, end_date
        )
        
        return {
            "spend": platform_data.get("spend", 0) if platform_data else 0,
            "impressions": platform_data.get("impressions", 0) if platform_data else 0,
            "clicks": platform_data.get("clicks", 0) if platform_data else 0,
            "conversions": platform_data.get("conversions", 0) if platform_data else 0,
            "campaigns": platform_data.get("campaigns", []) if platform_data else [],
            "daily_data": daily_data
        }
    
    async def _get_platform_data(
        self,
        db,
        user_id: str,
        platform: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[Dict[str, Any]]:
        """
        Get aggregated data for a specific platform from database
        """
        try:
            # Query ads data collection
            collection = db[f"ads_{platform}"]
            
            pipeline = [
                {
                    "$match": {
                        "user_id": user_id,
                        "date": {"$gte": start_date, "$lte": end_date}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "spend": {"$sum": "$spend"},
                        "impressions": {"$sum": "$impressions"},
                        "clicks": {"$sum": "$clicks"},
                        "conversions": {"$sum": "$conversions"}
                    }
                }
            ]
            
            result = await collection.aggregate(pipeline).to_list(length=1)
            
            if result:
                # Get campaigns for this platform
                campaigns = await self._get_campaigns(
                    db, user_id, platform, start_date, end_date
                )
                result[0]["campaigns"] = campaigns
                return result[0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting platform data for {platform}: {e}")
            return None
    
    async def _get_campaigns(
        self,
        db,
        user_id: str,
        platform: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get campaign-level data for a platform
        """
        try:
            collection = db[f"ads_{platform}_campaigns"]
            
            pipeline = [
                {
                    "$match": {
                        "user_id": user_id,
                        "date": {"$gte": start_date, "$lte": end_date}
                    }
                },
                {
                    "$group": {
                        "_id": "$campaign_id",
                        "name": {"$first": "$campaign_name"},
                        "spend": {"$sum": "$spend"},
                        "impressions": {"$sum": "$impressions"},
                        "clicks": {"$sum": "$clicks"},
                        "conversions": {"$sum": "$conversions"},
                        "revenue": {"$sum": "$revenue"}
                    }
                },
                {
                    "$addFields": {
                        "roas": {
                            "$cond": [
                                {"$gt": ["$spend", 0]},
                                {"$divide": ["$revenue", "$spend"]},
                                0
                            ]
                        }
                    }
                },
                {"$sort": {"spend": -1}},
                {"$limit": 20}
            ]
            
            campaigns = await collection.aggregate(pipeline).to_list(length=20)
            
            return [
                {
                    "campaign_id": str(c["_id"]),
                    "name": c.get("name", "Unknown"),
                    "platform": platform,
                    "spend": c.get("spend", 0),
                    "impressions": c.get("impressions", 0),
                    "clicks": c.get("clicks", 0),
                    "conversions": c.get("conversions", 0),
                    "roas": round(c.get("roas", 0), 2)
                }
                for c in campaigns
            ]
            
        except Exception as e:
            logger.error(f"Error getting campaigns for {platform}: {e}")
            return []
    
    async def _get_daily_breakdown(
        self,
        db,
        user_id: str,
        platform: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get daily breakdown of metrics for a platform
        """
        try:
            collection = db[f"ads_{platform}"]
            
            pipeline = [
                {
                    "$match": {
                        "user_id": user_id,
                        "date": {"$gte": start_date, "$lte": end_date}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "$dateToString": {"format": "%Y-%m-%d", "date": "$date"}
                        },
                        "spend": {"$sum": "$spend"},
                        "impressions": {"$sum": "$impressions"},
                        "clicks": {"$sum": "$clicks"},
                        "conversions": {"$sum": "$conversions"}
                    }
                },
                {"$sort": {"_id": 1}}
            ]
            
            daily_data = await collection.aggregate(pipeline).to_list(length=100)
            
            return [
                {
                    "date": d["_id"],
                    "spend": d.get("spend", 0),
                    "impressions": d.get("impressions", 0),
                    "clicks": d.get("clicks", 0),
                    "conversions": d.get("conversions", 0)
                }
                for d in daily_data
            ]
            
        except Exception as e:
            logger.error(f"Error getting daily breakdown for {platform}: {e}")
            return []


