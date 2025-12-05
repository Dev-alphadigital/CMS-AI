"""
Branding Aggregator - Aggregates branding metrics across all social platforms
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from app.core.database import get_database
import logging

logger = logging.getLogger(__name__)


class BrandingAggregator:
    """
    Aggregates branding metrics from multiple social media platforms:
    - Facebook
    - Instagram
    - Twitter/X
    - LinkedIn
    - TikTok
    - YouTube
    """
    
    SUPPORTED_PLATFORMS = [
        "facebook",
        "instagram",
        "twitter",
        "linkedin",
        "tiktok",
        "youtube"
    ]
    
    async def aggregate_all_platforms(
        self,
        user_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Aggregate branding metrics from all connected platforms
        
        Args:
            user_id: User ID
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Aggregated branding statistics across all platforms
        """
        db = await get_database()
        
        # Build query filter
        query_filter = {"user_id": user_id}
        
        if start_date or end_date:
            query_filter["date"] = {}
            if start_date:
                query_filter["date"]["$gte"] = start_date.strftime("%Y-%m-%d")
            if end_date:
                query_filter["date"]["$lte"] = end_date.strftime("%Y-%m-%d")
        
        # Get latest metrics
        latest_metrics = await db.branding_metrics.find_one(
            query_filter,
            sort=[("date", -1)]
        )
        
        if not latest_metrics:
            return {
                "total_followers": 0,
                "total_engagement": 0,
                "total_posts": 0,
                "avg_engagement_rate": 0.0,
                "platform_breakdown": {},
                "sentiment_score": 0.0
            }
        
        platforms = latest_metrics.get("platforms", {})
        
        total_followers = 0
        total_engagement = 0
        total_posts = 0
        total_likes = 0
        total_comments = 0
        total_shares = 0
        
        platform_breakdown = {}
        
        for platform_name, platform_data in platforms.items():
            followers = platform_data.get("followers", 0)
            engagement_rate = platform_data.get("engagement_rate", 0)
            posts = platform_data.get("posts", 0)
            likes = platform_data.get("likes", 0)
            comments = platform_data.get("comments", 0)
            shares = platform_data.get("shares", 0)
            
            total_followers += followers
            total_posts += posts
            total_likes += likes
            total_comments += comments
            total_shares += shares
            
            # Calculate engagement for this platform
            platform_engagement = likes + comments + shares
            total_engagement += platform_engagement
            
            platform_breakdown[platform_name] = {
                "followers": followers,
                "engagement_rate": engagement_rate,
                "posts": posts,
                "likes": likes,
                "comments": comments,
                "shares": shares,
                "total_engagement": platform_engagement
            }
        
        # Calculate average engagement rate
        engagement_rates = [
            p.get("engagement_rate", 0)
            for p in platform_breakdown.values()
            if p.get("engagement_rate", 0) > 0
        ]
        avg_engagement_rate = (
            sum(engagement_rates) / len(engagement_rates)
            if engagement_rates else 0.0
        )
        
        sentiment_score = latest_metrics.get("sentiment_score", 0.0)
        
        return {
            "total_followers": total_followers,
            "total_engagement": total_engagement,
            "total_posts": total_posts,
            "total_likes": total_likes,
            "total_comments": total_comments,
            "total_shares": total_shares,
            "avg_engagement_rate": round(avg_engagement_rate, 2),
            "platform_breakdown": platform_breakdown,
            "sentiment_score": round(sentiment_score, 2)
        }
    
    async def aggregate_single_platform(
        self,
        user_id: str,
        platform: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get aggregated data for a single platform
        
        Args:
            user_id: User ID
            platform: Platform name
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Aggregated statistics for the platform
        """
        if platform not in self.SUPPORTED_PLATFORMS:
            raise ValueError(f"Unsupported platform: {platform}")
        
        db = await get_database()
        
        query_filter = {"user_id": user_id}
        
        if start_date or end_date:
            query_filter["date"] = {}
            if start_date:
                query_filter["date"]["$gte"] = start_date.strftime("%Y-%m-%d")
            if end_date:
                query_filter["date"]["$lte"] = end_date.strftime("%Y-%m-%d")
        
        latest_metrics = await db.branding_metrics.find_one(
            query_filter,
            sort=[("date", -1)]
        )
        
        if not latest_metrics:
            return {
                "platform": platform,
                "followers": 0,
                "engagement_rate": 0.0,
                "posts": 0,
                "likes": 0,
                "comments": 0,
                "shares": 0
            }
        
        platform_data = latest_metrics.get("platforms", {}).get(platform, {})
        
        return {
            "platform": platform,
            "followers": platform_data.get("followers", 0),
            "engagement_rate": platform_data.get("engagement_rate", 0.0),
            "posts": platform_data.get("posts", 0),
            "likes": platform_data.get("likes", 0),
            "comments": platform_data.get("comments", 0),
            "shares": platform_data.get("shares", 0),
            "total_engagement": (
                platform_data.get("likes", 0) +
                platform_data.get("comments", 0) +
                platform_data.get("shares", 0)
            )
        }
    
    async def get_growth_timeline(
        self,
        user_id: str,
        start_date: datetime,
        end_date: datetime,
        platform: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get follower growth timeline
        
        Args:
            user_id: User ID
            platform: Optional platform filter
            start_date: Start date
            end_date: End date
            
        Returns:
            List of follower counts over time
        """
        db = await get_database()
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        metrics_cursor = db.branding_metrics.find(query_filter).sort("date", 1)
        metrics_data = await metrics_cursor.to_list(length=None)
        
        timeline = []
        
        for metric in metrics_data:
            date = metric.get("date")
            platforms = metric.get("platforms", {})
            
            if platform:
                # Single platform
                platform_data = platforms.get(platform, {})
                timeline.append({
                    "date": date,
                    "platform": platform,
                    "followers": platform_data.get("followers", 0)
                })
            else:
                # All platforms combined
                total_followers = sum(
                    p.get("followers", 0)
                    for p in platforms.values()
                )
                timeline.append({
                    "date": date,
                    "platform": "all",
                    "followers": total_followers
                })
        
        return timeline
    
    async def calculate_growth_metrics(
        self,
        user_id: str,
        start_date: datetime,
        end_date: datetime,
        platform: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Calculate growth metrics for a period
        
        Args:
            user_id: User ID
            start_date: Start date
            end_date: End date
            platform: Optional platform filter
            
        Returns:
            Growth metrics (net growth, growth rate, etc.)
        """
        timeline = await self.get_growth_timeline(
            user_id, start_date, end_date, platform
        )
        
        if len(timeline) < 2:
            return {
                "net_growth": 0,
                "growth_rate": 0.0,
                "current_followers": 0,
                "starting_followers": 0,
                "daily_avg_growth": 0.0
            }
        
        first_followers = timeline[0]["followers"]
        last_followers = timeline[-1]["followers"]
        net_growth = last_followers - first_followers
        
        growth_rate = (
            (net_growth / first_followers * 100)
            if first_followers > 0 else 0.0
        )
        
        # Calculate daily average growth
        days = (end_date - start_date).days
        daily_avg_growth = net_growth / days if days > 0 else 0.0
        
        return {
            "net_growth": net_growth,
            "growth_rate": round(growth_rate, 2),
            "current_followers": last_followers,
            "starting_followers": first_followers,
            "daily_avg_growth": round(daily_avg_growth, 2),
            "timeline_points": len(timeline)
        }
    
    async def get_engagement_summary(
        self,
        user_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Get engagement summary across all platforms
        
        Args:
            user_id: User ID
            start_date: Start date
            end_date: End date
            
        Returns:
            Engagement summary with platform breakdown
        """
        db = await get_database()
        
        query_filter = {
            "user_id": user_id,
            "date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        metrics_cursor = db.branding_metrics.find(query_filter)
        metrics_data = await metrics_cursor.to_list(length=None)
        
        platform_engagement = defaultdict(lambda: {
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "total_engagement": 0
        })
        
        total_likes = 0
        total_comments = 0
        total_shares = 0
        
        for metric in metrics_data:
            platforms = metric.get("platforms", {})
            
            for platform_name, platform_data in platforms.items():
                likes = platform_data.get("likes", 0)
                comments = platform_data.get("comments", 0)
                shares = platform_data.get("shares", 0)
                
                total_likes += likes
                total_comments += comments
                total_shares += shares
                
                platform_engagement[platform_name]["likes"] += likes
                platform_engagement[platform_name]["comments"] += comments
                platform_engagement[platform_name]["shares"] += shares
                platform_engagement[platform_name]["total_engagement"] += (
                    likes + comments + shares
                )
        
        # Format platform engagement
        platform_list = [
            {
                "platform": platform,
                **data
            }
            for platform, data in platform_engagement.items()
        ]
        
        # Sort by total engagement
        platform_list.sort(key=lambda x: x["total_engagement"], reverse=True)
        
        return {
            "total_likes": total_likes,
            "total_comments": total_comments,
            "total_shares": total_shares,
            "total_engagement": total_likes + total_comments + total_shares,
            "platform_breakdown": platform_list
        }

