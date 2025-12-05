"""
Ads Manager API endpoints
Handles multi-platform ad analytics, predictions, and recommendations
"""

from fastapi import APIRouter
from .overview import router as overview_router
from .analytics import router as analytics_router
from .campaigns import router as campaigns_router
from .predictions import router as predictions_router
from .recommendations import router as recommendations_router

router = APIRouter()

# Include all ads sub-routers
router.include_router(overview_router, tags=["Ads Overview"])
router.include_router(analytics_router, tags=["Ads Analytics"])
router.include_router(campaigns_router, tags=["Ads Campaigns"])
router.include_router(predictions_router, tags=["Ads Predictions"])
router.include_router(recommendations_router, tags=["Ads Recommendations"])