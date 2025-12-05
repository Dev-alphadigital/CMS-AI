"""
SEO API endpoints
Handles keyword tracking, rankings, traffic analysis, and growth predictions
"""

from fastapi import APIRouter
from .overview import router as overview_router
from .keywords import router as keywords_router
from .rankings import router as rankings_router
from .traffic import router as traffic_router
from .growth_report import router as growth_report_router
from .competitors import router as competitors_router

router = APIRouter()

# Include all SEO sub-routers
router.include_router(overview_router, tags=["SEO Overview"])
router.include_router(keywords_router, tags=["SEO Keywords"])
router.include_router(rankings_router, tags=["SEO Rankings"])
router.include_router(traffic_router, tags=["SEO Traffic"])
router.include_router(growth_report_router, tags=["SEO Growth Report"])
router.include_router(competitors_router, tags=["SEO Competitors"])