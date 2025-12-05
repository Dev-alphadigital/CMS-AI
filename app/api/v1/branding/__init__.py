"""
Branding API endpoints
Handles social media metrics, brand sentiment, and content performance
"""

from fastapi import APIRouter
from .overview import router as overview_router
from .social_analytics import router as social_analytics_router
from .scheduled_posts import router as scheduled_posts_router
from .sentiment import router as sentiment_router

router = APIRouter()

# Include all branding sub-routers
router.include_router(overview_router, tags=["Branding Overview"])
router.include_router(social_analytics_router, tags=["Social Analytics"])
router.include_router(scheduled_posts_router, tags=["Scheduled Posts"])
router.include_router(sentiment_router, tags=["Brand Sentiment"])