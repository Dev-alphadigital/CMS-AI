"""
Validation utilities
"""

import re
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def validate_email(email: str) -> bool:
    """
    Validate email format
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def validate_phone(phone: str) -> bool:
    """
    Validate phone number format (basic validation)
    """
    # Remove common separators
    cleaned = re.sub(r'[\s\-\(\)]', '', phone)
    
    # Check if contains only digits and + (for international)
    pattern = r'^\+?[0-9]{10,15}$'
    return re.match(pattern, cleaned) is not None


def validate_url(url: str) -> bool:
    """
    Validate URL format
    """
    pattern = r'^https?://[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}(/.*)?$'
    return re.match(pattern, url) is not None


def validate_domain(domain: str) -> bool:
    """
    Validate domain name
    """
    pattern = r'^[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, domain) is not None


def validate_date_format(date_string: str, format: str = "%Y-%m-%d") -> bool:
    """
    Validate date string format
    """
    from datetime import datetime
    try:
        datetime.strptime(date_string, format)
        return True
    except ValueError:
        return False


def sanitize_string(text: str, max_length: Optional[int] = None) -> str:
    """
    Sanitize string input
    """
    # Remove control characters
    sanitized = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
    
    # Trim whitespace
    sanitized = sanitized.strip()
    
    # Limit length if specified
    if max_length and len(sanitized) > max_length:
        sanitized = sanitized[:max_length]
    
    return sanitized


def validate_api_key_format(api_key: str, platform: str) -> bool:
    """
    Validate API key format for different platforms
    """
    validations = {
        "meta": lambda k: len(k) > 20,
        "google": lambda k: len(k) > 20,
        "twitter": lambda k: len(k) > 20,
        "tiktok": lambda k: len(k) > 20,
        "linkedin": lambda k: len(k) > 20
    }
    
    validator = validations.get(platform.lower())
    if validator:
        return validator(api_key)
    
    # Default validation
    return len(api_key) > 10