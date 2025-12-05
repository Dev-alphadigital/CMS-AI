"""
Email Marketing API endpoints
Handles email campaign management, analytics, and scheduling
"""

from fastapi import APIRouter
from .campaigns import router as campaigns_router
from .analytics import router as analytics_router
from .scheduled import router as scheduled_router

router = APIRouter()

# Include all email marketing sub-routers
router.include_router(campaigns_router, tags=["Email Campaigns"])
router.include_router(analytics_router, tags=["Email Analytics"])
router.include_router(scheduled_router, tags=["Scheduled Campaigns"])