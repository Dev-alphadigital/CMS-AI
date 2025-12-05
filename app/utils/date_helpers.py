
"""
Date and time helper functions
"""

from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


def get_date_range(range_type: str, start_date: str = None, end_date: str = None) -> Dict[str, datetime]:
    """
    Get start and end dates for common date ranges
    """
    end = datetime.utcnow()
    
    if range_type == "custom" and start_date and end_date:
        try:
            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)
        except ValueError:
            logger.error(f"Invalid date format: {start_date} or {end_date}")
            start = end - timedelta(days=7)
    elif range_type == "today":
        start = end.replace(hour=0, minute=0, second=0, microsecond=0)
    elif range_type == "yesterday":
        start = (end - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
    elif range_type == "last_7_days":
        start = end - timedelta(days=7)
    elif range_type == "last_30_days":
        start = end - timedelta(days=30)
    elif range_type == "last_90_days":
        start = end - timedelta(days=90)
    elif range_type == "this_week":
        start = end - timedelta(days=end.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    elif range_type == "this_month":
        start = end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif range_type == "this_year":
        start = end.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start = end - timedelta(days=7)  # Default to last 7 days
    
    return {"start_date": start, "end_date": end}


def get_previous_period(start_date: datetime, end_date: datetime) -> Dict[str, datetime]:
    """
    Get previous period dates for comparison
    """
    period_length = (end_date - start_date).days
    previous_end = start_date
    previous_start = previous_end - timedelta(days=period_length)
    
    return {"start_date": previous_start, "end_date": previous_end}


def calculate_percentage_change(previous: float, current: float) -> float:
    """
    Calculate percentage change between two values
    """
    if previous == 0:
        return 0 if current == 0 else 100
    return round(((current - previous) / previous * 100), 2)


def is_business_hours(dt: datetime = None, tz: str = "UTC") -> bool:
    """
    Check if datetime is within business hours (9 AM - 5 PM)
    """
    if dt is None:
        dt = datetime.utcnow()
    
    # Business hours: Monday-Friday, 9 AM - 5 PM
    if dt.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    hour = dt.hour
    return 9 <= hour < 17


def get_time_buckets(start: datetime, end: datetime, bucket_size: str = "day") -> List[Tuple[datetime, datetime]]:
    """
    Generate time buckets between start and end dates
    """
    buckets = []
    current = start
    
    if bucket_size == "hour":
        delta = timedelta(hours=1)
    elif bucket_size == "day":
        delta = timedelta(days=1)
    elif bucket_size == "week":
        delta = timedelta(weeks=1)
    elif bucket_size == "month":
        delta = timedelta(days=30)  # Approximate
    else:
        delta = timedelta(days=1)
    
    while current < end:
        bucket_end = min(current + delta, end)
        buckets.append((current, bucket_end))
        current = bucket_end
    
    return buckets


def get_weekday_name(date: datetime) -> str:
    """
    Get weekday name from datetime
    """
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return days[date.weekday()]


def get_month_name(date: datetime) -> str:
    """
    Get month name from datetime
    """
    months = ["January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"]
    return months[date.month - 1]