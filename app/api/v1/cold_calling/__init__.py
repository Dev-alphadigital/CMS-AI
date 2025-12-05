"""
Cold Calling API endpoints
Handles call tracking, analytics, and agent performance monitoring
"""

from fastapi import APIRouter
from .overview import router as overview_router
from .history import router as history_router
from .analytics import router as analytics_router

router = APIRouter()

# Include all cold calling sub-routers
router.include_router(overview_router, tags=["Cold Calling Overview"])
router.include_router(history_router, tags=["Call History"])
router.include_router(analytics_router, tags=["Call Analytics"])