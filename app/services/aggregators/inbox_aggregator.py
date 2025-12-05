"""
Inbox Aggregator - Aggregates inbox messages across all platforms
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
from app.core.database import get_database
import logging

logger = logging.getLogger(__name__)


class InboxAggregator:
    """
    Aggregates inbox messages from multiple platforms:
    - Facebook Messenger
    - Instagram Direct Messages
    - WhatsApp
    - Twitter/X DMs
    - Email
    """
    
    SUPPORTED_PLATFORMS = [
        "facebook_messenger",
        "instagram",
        "whatsapp",
        "twitter",
        "email"
    ]
    
    async def aggregate_all_platforms(
        self,
        user_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Aggregate inbox messages from all connected platforms
        
        Args:
            user_id: User ID
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Aggregated inbox statistics across all platforms
        """
        db = await get_database()
        
        # Build query filter
        query_filter = {"user_id": user_id}
        
        if start_date or end_date:
            query_filter["timestamp"] = {}
            if start_date:
                query_filter["timestamp"]["$gte"] = start_date
            if end_date:
                query_filter["timestamp"]["$lte"] = end_date
        
        # Aggregate by platform
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": "$platform",
                "total_messages": {"$sum": 1},
                "unread_count": {
                    "$sum": {"$cond": [{"$eq": ["$read", False]}, 1, 0]}
                },
                "replied_count": {
                    "$sum": {"$cond": [{"$eq": ["$replied", True]}, 1, 0]}
                },
                "archived_count": {
                    "$sum": {"$cond": [{"$eq": ["$archived", True]}, 1, 0]}
                },
                "high_priority_count": {
                    "$sum": {"$cond": [{"$eq": ["$priority", "high"]}, 1, 0]}
                },
                "medium_priority_count": {
                    "$sum": {"$cond": [{"$eq": ["$priority", "medium"]}, 1, 0]}
                },
                "low_priority_count": {
                    "$sum": {"$cond": [{"$eq": ["$priority", "low"]}, 1, 0]}
                }
            }},
            {"$sort": {"total_messages": -1}}
        ]
        
        platform_stats = await db.inbox_messages.aggregate(pipeline).to_list(length=None)
        
        # Calculate totals
        total_messages = 0
        total_unread = 0
        total_replied = 0
        total_archived = 0
        
        platform_breakdown = {}
        
        for stat in platform_stats:
            platform = stat["_id"]
            platform_breakdown[platform] = {
                "total_messages": stat["total_messages"],
                "unread_count": stat["unread_count"],
                "replied_count": stat["replied_count"],
                "archived_count": stat["archived_count"],
                "high_priority": stat["high_priority_count"],
                "medium_priority": stat["medium_priority_count"],
                "low_priority": stat["low_priority_count"]
            }
            
            total_messages += stat["total_messages"]
            total_unread += stat["unread_count"]
            total_replied += stat["replied_count"]
            total_archived += stat["archived_count"]
        
        # Get response time metrics
        response_times = await self._calculate_response_times(db, user_id, start_date, end_date)
        
        # Get peak hours
        peak_hours = await self._get_peak_hours(db, user_id, start_date, end_date)
        
        return {
            "total_messages": total_messages,
            "total_unread": total_unread,
            "total_replied": total_replied,
            "total_archived": total_archived,
            "read_percentage": round(
                ((total_messages - total_unread) / total_messages * 100) if total_messages > 0 else 0,
                2
            ),
            "reply_percentage": round(
                (total_replied / total_messages * 100) if total_messages > 0 else 0,
                2
            ),
            "platform_breakdown": platform_breakdown,
            "response_times": response_times,
            "peak_hours": peak_hours
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
        
        query_filter = {"user_id": user_id, "platform": platform}
        
        if start_date or end_date:
            query_filter["timestamp"] = {}
            if start_date:
                query_filter["timestamp"]["$gte"] = start_date
            if end_date:
                query_filter["timestamp"]["$lte"] = end_date
        
        pipeline = [
            {"$match": query_filter},
            {"$group": {
                "_id": None,
                "total_messages": {"$sum": 1},
                "unread_count": {
                    "$sum": {"$cond": [{"$eq": ["$read", False]}, 1, 0]}
                },
                "replied_count": {
                    "$sum": {"$cond": [{"$eq": ["$replied", True]}, 1, 0]}
                },
                "archived_count": {
                    "$sum": {"$cond": [{"$eq": ["$archived", True]}, 1, 0]}
                }
            }}
        ]
        
        result = await db.inbox_messages.aggregate(pipeline).to_list(length=1)
        
        if result:
            stats = result[0]
            return {
                "platform": platform,
                "total_messages": stats["total_messages"],
                "unread_count": stats["unread_count"],
                "replied_count": stats["replied_count"],
                "archived_count": stats["archived_count"],
                "read_percentage": round(
                    ((stats["total_messages"] - stats["unread_count"]) / stats["total_messages"] * 100)
                    if stats["total_messages"] > 0 else 0,
                    2
                )
            }
        
        return {
            "platform": platform,
            "total_messages": 0,
            "unread_count": 0,
            "replied_count": 0,
            "archived_count": 0,
            "read_percentage": 0.0
        }
    
    async def _calculate_response_times(
        self,
        db,
        user_id: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ) -> Dict[str, Any]:
        """
        Calculate average response times for messages
        """
        try:
            query_filter = {
                "user_id": user_id,
                "replied": True,
                "replied_at": {"$exists": True},
                "timestamp": {"$exists": True}
            }
            
            if start_date or end_date:
                query_filter["timestamp"] = {}
                if start_date:
                    query_filter["timestamp"]["$gte"] = start_date
                if end_date:
                    query_filter["timestamp"]["$lte"] = end_date
            
            pipeline = [
                {"$match": query_filter},
                {"$project": {
                    "response_time_hours": {
                        "$divide": [
                            {"$subtract": ["$replied_at", "$timestamp"]},
                            3600000  # Convert milliseconds to hours
                        ]
                    }
                }},
                {"$group": {
                    "_id": None,
                    "avg_response_time_hours": {"$avg": "$response_time_hours"},
                    "min_response_time_hours": {"$min": "$response_time_hours"},
                    "max_response_time_hours": {"$max": "$response_time_hours"}
                }}
            ]
            
            result = await db.inbox_messages.aggregate(pipeline).to_list(length=1)
            
            if result and result[0].get("avg_response_time_hours"):
                return {
                    "avg_hours": round(result[0]["avg_response_time_hours"], 2),
                    "min_hours": round(result[0].get("min_response_time_hours", 0), 2),
                    "max_hours": round(result[0].get("max_response_time_hours", 0), 2)
                }
            
            return {
                "avg_hours": 0.0,
                "min_hours": 0.0,
                "max_hours": 0.0
            }
            
        except Exception as e:
            logger.error(f"Error calculating response times: {e}")
            return {
                "avg_hours": 0.0,
                "min_hours": 0.0,
                "max_hours": 0.0
            }
    
    async def _get_peak_hours(
        self,
        db,
        user_id: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ) -> List[Dict[str, Any]]:
        """
        Get hourly distribution of messages (peak hours)
        """
        try:
            query_filter = {"user_id": user_id}
            
            if start_date or end_date:
                query_filter["timestamp"] = {}
                if start_date:
                    query_filter["timestamp"]["$gte"] = start_date
                if end_date:
                    query_filter["timestamp"]["$lte"] = end_date
            
            pipeline = [
                {"$match": query_filter},
                {"$group": {
                    "_id": {"$hour": "$timestamp"},
                    "count": {"$sum": 1}
                }},
                {"$sort": {"_id": 1}}
            ]
            
            hourly_data = await db.inbox_messages.aggregate(pipeline).to_list(length=24)
            
            return [
                {"hour": item["_id"], "message_count": item["count"]}
                for item in hourly_data
            ]
            
        except Exception as e:
            logger.error(f"Error getting peak hours: {e}")
            return []
    
    async def get_conversation_summary(
        self,
        user_id: str,
        sender_id: str,
        platform: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get summary of a conversation thread
        
        Args:
            user_id: User ID
            sender_id: Sender ID to get conversation with
            platform: Optional platform filter
            
        Returns:
            Conversation summary with message count, last message, etc.
        """
        db = await get_database()
        
        query_filter = {
            "user_id": user_id,
            "sender.id": sender_id
        }
        
        if platform:
            query_filter["platform"] = platform
        
        # Get conversation stats
        pipeline = [
            {"$match": query_filter},
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": None,
                "total_messages": {"$sum": 1},
                "unread_count": {
                    "$sum": {"$cond": [{"$eq": ["$read", False]}, 1, 0]}
                },
                "last_message": {"$first": "$$ROOT"},
                "first_message": {"$last": "$$ROOT"}
            }}
        ]
        
        result = await db.inbox_messages.aggregate(pipeline).to_list(length=1)
        
        if result:
            summary = result[0]
            return {
                "sender_id": sender_id,
                "platform": platform,
                "total_messages": summary["total_messages"],
                "unread_count": summary["unread_count"],
                "last_message_time": summary["last_message"].get("timestamp"),
                "first_message_time": summary["first_message"].get("timestamp"),
                "replied": summary["last_message"].get("replied", False)
            }
        
        return {
            "sender_id": sender_id,
            "platform": platform,
            "total_messages": 0,
            "unread_count": 0
        }

