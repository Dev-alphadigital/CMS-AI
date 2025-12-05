"""
Data formatting utilities
"""

import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


def format_currency(amount: float, currency: str = "USD") -> str:
    """
    Format currency amount
    """
    symbols = {
        "USD": "$",
        "EUR": "€",
        "GBP": "£",
        "JPY": "¥"
    }
    
    symbol = symbols.get(currency, "$")
    
    if amount >= 1000000:
        return f"{symbol}{amount/1000000:.2f}M"
    elif amount >= 1000:
        return f"{symbol}{amount/1000:.2f}K"
    else:
        return f"{symbol}{amount:.2f}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """
    Format percentage
    """
    return f"{value:.{decimals}f}%"


def format_number(value: float, decimals: int = 2) -> str:
    """
    Format number with thousand separators
    """
    return f"{value:,.{decimals}f}"


def format_duration(seconds: int) -> str:
    """
    Format duration in seconds to human readable format
    """
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"


def format_date(date: datetime, format: str = "%Y-%m-%d") -> str:
    """
    Format datetime to string
    """
    return date.strftime(format)


def format_datetime(dt: datetime, format: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Format datetime to string with time
    """
    return dt.strftime(format)


def format_relative_time(dt: datetime) -> str:
    """
    Format datetime as relative time (e.g., "2 hours ago")
    """
    now = datetime.utcnow()
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 60:
        return "just now"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours} hour{'s' if hours > 1 else ''} ago"
    elif seconds < 604800:
        days = int(seconds / 86400)
        return f"{days} day{'s' if days > 1 else ''} ago"
    elif seconds < 2592000:
        weeks = int(seconds / 604800)
        return f"{weeks} week{'s' if weeks > 1 else ''} ago"
    else:
        months = int(seconds / 2592000)
        return f"{months} month{'s' if months > 1 else ''} ago"


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncate string with suffix
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def format_phone_number(phone: str, format: str = "US") -> str:
    """
    Format phone number
    """
    # Remove non-digits
    digits = re.sub(r'\D', '', phone)
    
    if format == "US" and len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif format == "US" and len(digits) == 11 and digits[0] == "1":
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    
    return phone


def format_list_to_sentence(items: List[str]) -> str:
    """
    Format list as comma-separated sentence
    """
    if not items:
        return ""
    elif len(items) == 1:
        return items[0]
    elif len(items) == 2:
        return f"{items[0]} and {items[1]}"
    else:
        return f"{', '.join(items[:-1])}, and {items[-1]}"