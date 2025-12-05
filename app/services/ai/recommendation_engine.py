"""
Recommendation Engine - AI-powered optimization recommendations
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from .llm_client import LLMClient

logger = logging.getLogger(__name__)


class RecommendationEngine:
    """
    Generate AI-powered recommendations for ads, SEO, etc.
    """
    
    def __init__(self, provider: str = "openai"):
        self.llm = LLMClient(provider=provider)
    
    async def generate_ads_recommendations(
        self,
        campaign_data: List[Dict[str, Any]],
        account_performance: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generate recommendations for ad campaigns
        
        Args:
            campaign_data: Recent campaign performance
            account_performance: Overall account metrics
            
        Returns:
            List of recommendations with priority and expected impact
        """
        try:
            # Prepare data summary for LLM
            data_summary = self._prepare_ads_summary(campaign_data, account_performance)
            
            system_prompt = """You are an expert digital advertising strategist. 
Analyze campaign performance data and provide actionable optimization recommendations.
Focus on ROI, CTR, conversion rates, and budget allocation."""
            
            prompt = f"""Analyze this advertising performance data and provide 3-5 specific, actionable recommendations:

{data_summary}

For each recommendation, provide:
1. Clear action to take
2. Expected impact (high/medium/low)
3. Priority (high/medium/low)
4. Specific metrics that will improve

Respond with ONLY valid JSON in this format:
{{
    "recommendations": [
        {{
            "title": "Brief title",
            "description": "Detailed recommendation",
            "priority": "high" | "medium" | "low",
            "expected_impact": "high" | "medium" | "low",
            "metrics_affected": ["metric1", "metric2"],
            "action_items": ["action1", "action2"]
        }}
    ]
}}"""
            
            schema = {
                "recommendations": [
                    {
                        "title": "string",
                        "description": "string",
                        "priority": "string",
                        "expected_impact": "string",
                        "metrics_affected": ["string"],
                        "action_items": ["string"]
                    }
                ]
            }
            
            response = await self.llm.generate_structured_output(
                prompt=prompt,
                output_schema=schema,
                system_prompt=system_prompt
            )
            
            recommendations = response.get("recommendations", [])
            
            # Add metadata
            for rec in recommendations:
                rec["generated_at"] = datetime.utcnow().isoformat()
                rec["category"] = "ads_optimization"
                rec["status"] = "pending"
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate ads recommendations: {str(e)}")
            return []
    
    async def generate_seo_recommendations(
        self,
        seo_data: Dict[str, Any],
        keyword_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Generate SEO optimization recommendations
        """
        try:
            data_summary = self._prepare_seo_summary(seo_data, keyword_data)
            
            system_prompt = """You are an expert SEO strategist.
Analyze SEO performance data and provide actionable optimization recommendations.
Focus on keyword rankings, organic traffic, and technical SEO."""
            
            prompt = f"""Analyze this SEO performance data and provide 3-5 specific recommendations:

{data_summary}

Respond with ONLY valid JSON in this format:
{{
    "recommendations": [
        {{
            "title": "Brief title",
            "description": "Detailed recommendation",
            "priority": "high" | "medium" | "low",
            "expected_impact": "high" | "medium" | "low",
            "metrics_affected": ["metric1", "metric2"],
            "action_items": ["action1", "action2"]
        }}
    ]
}}"""
            
            schema = {
                "recommendations": [
                    {
                        "title": "string",
                        "description": "string",
                        "priority": "string",
                        "expected_impact": "string",
                        "metrics_affected": ["string"],
                        "action_items": ["string"]
                    }
                ]
            }
            
            response = await self.llm.generate_structured_output(
                prompt=prompt,
                output_schema=schema,
                system_prompt=system_prompt
            )
            
            recommendations = response.get("recommendations", [])
            
            for rec in recommendations:
                rec["generated_at"] = datetime.utcnow().isoformat()
                rec["category"] = "seo_optimization"
                rec["status"] = "pending"
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate SEO recommendations: {str(e)}")
            return []
    
    async def generate_email_recommendations(
        self,
        campaign_performance: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Generate email marketing recommendations
        """
        try:
            data_summary = self._prepare_email_summary(campaign_performance)
            
            system_prompt = """You are an expert email marketing strategist.
Analyze email campaign performance and provide recommendations to improve open rates, 
click rates, and conversions."""
            
            prompt = f"""Analyze this email marketing data and provide 3-5 recommendations:

{data_summary}

Respond with ONLY valid JSON in this format:
{{
    "recommendations": [
        {{
            "title": "Brief title",
            "description": "Detailed recommendation",
            "priority": "high" | "medium" | "low",
            "expected_impact": "high" | "medium" | "low",
            "metrics_affected": ["metric1", "metric2"],
            "action_items": ["action1", "action2"]
        }}
    ]
}}"""
            
            schema = {
                "recommendations": [
                    {
                        "title": "string",
                        "description": "string",
                        "priority": "string",
                        "expected_impact": "string",
                        "metrics_affected": ["string"],
                        "action_items": ["string"]
                    }
                ]
            }
            
            response = await self.llm.generate_structured_output(
                prompt=prompt,
                output_schema=schema,
                system_prompt=system_prompt
            )
            
            recommendations = response.get("recommendations", [])
            
            for rec in recommendations:
                rec["generated_at"] = datetime.utcnow().isoformat()
                rec["category"] = "email_marketing"
                rec["status"] = "pending"
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate email recommendations: {str(e)}")
            return []
    
    def _prepare_ads_summary(
        self,
        campaign_data: List[Dict[str, Any]],
        account_performance: Dict[str, Any]
    ) -> str:
        """Prepare ads data summary for LLM"""
        summary = f"""
ACCOUNT PERFORMANCE:
- Total Spend: ${account_performance.get('total_spend', 0):,.2f}
- Total Clicks: {account_performance.get('total_clicks', 0):,}
- Total Conversions: {account_performance.get('total_conversions', 0)}
- Average CTR: {account_performance.get('avg_ctr', 0):.2f}%
- Average CPC: ${account_performance.get('avg_cpc', 0):.2f}
- Average ROAS: {account_performance.get('avg_roas', 0):.2f}x

TOP CAMPAIGNS:
"""
        for i, campaign in enumerate(campaign_data[:5], 1):
            summary += f"""
{i}. {campaign.get('campaign_name', 'Unknown')}
   - Spend: ${campaign.get('spend', 0):,.2f}
   - Clicks: {campaign.get('clicks', 0):,}
   - CTR: {campaign.get('ctr', 0):.2f}%
   - Conversions: {campaign.get('conversions', 0)}
   - ROAS: {campaign.get('roas', 0):.2f}x
"""
        
        return summary.strip()
    
    def _prepare_seo_summary(
        self,
        seo_data: Dict[str, Any],
        keyword_data: List[Dict[str, Any]]
    ) -> str:
        """Prepare SEO data summary for LLM"""
        summary = f"""
OVERALL SEO PERFORMANCE:
- Organic Traffic: {seo_data.get('organic_traffic', 0):,} visits
- Total Keywords Ranking: {seo_data.get('total_keywords', 0)}
- Keywords in Top 10: {seo_data.get('top_10_keywords', 0)}
- Average Position: {seo_data.get('avg_position', 0):.1f}
- Total Backlinks: {seo_data.get('backlinks', 0):,}

TOP KEYWORDS:
"""
        for i, keyword in enumerate(keyword_data[:10], 1):
            summary += f"""
{i}. "{keyword.get('keyword', '')}" 
   - Position: {keyword.get('position', 0)}
   - Volume: {keyword.get('volume', 0):,}
   - Clicks: {keyword.get('clicks', 0)}
"""
        
        return summary.strip()
    
    def _prepare_email_summary(
        self,
        campaign_performance: List[Dict[str, Any]]
    ) -> str:
        """Prepare email data summary for LLM"""
        # Calculate aggregates
        total_sent = sum(c.get('sent', 0) for c in campaign_performance)
        total_opened = sum(c.get('opened', 0) for c in campaign_performance)
        total_clicked = sum(c.get('clicked', 0) for c in campaign_performance)
        
        avg_open_rate = (total_opened / total_sent * 100) if total_sent > 0 else 0
        avg_click_rate = (total_clicked / total_sent * 100) if total_sent > 0 else 0
        
        summary = f"""
OVERALL EMAIL PERFORMANCE:
- Total Campaigns: {len(campaign_performance)}
- Total Sent: {total_sent:,}
- Average Open Rate: {avg_open_rate:.2f}%
- Average Click Rate: {avg_click_rate:.2f}%

RECENT CAMPAIGNS:
"""
        for i, campaign in enumerate(campaign_performance[:5], 1):
            summary += f"""
{i}. {campaign.get('campaign_name', 'Unknown')}
   - Subject: {campaign.get('subject', 'N/A')}
   - Sent: {campaign.get('sent', 0):,}
   - Open Rate: {campaign.get('open_rate', 0):.2f}%
   - Click Rate: {campaign.get('click_rate', 0):.2f}%
"""
        
        return summary.strip()
    
    async def close(self):
        """Close LLM client"""
        await self.llm.close()