"""
Inbox API endpoints
Handles unified omnichannel messaging across all platforms
"""

from fastapi import APIRouter
from .messages import router as messages_router
from .filters import router as filters_router

router = APIRouter()

# Include all inbox sub-routers
router.include_router(messages_router, tags=["Inbox Messages"])
router.include_router(filters_router, tags=["Inbox Filters"])