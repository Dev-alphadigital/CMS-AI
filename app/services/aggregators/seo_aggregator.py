"""
SEO Aggregator - Aggregates SEO data from various sources
"""

from typing import Dict, Any, List
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class SEOAggregator:
    """
    Aggregates SEO metrics from Google Search Console and other SEO tools
    """
    
    def aggregate_seo_data(self, seo_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate SEO metrics from raw data
        
        Args:
            seo_data: List of SEO metric documents from database
            
        Returns:
            Aggregated SEO metrics dictionary
        """
        if not seo_data:
            return {
                "total_organic_traffic": 0,
                "total_impressions": 0,
                "total_clicks": 0,
                "total_keywords": 0,
                "top_10_keywords": 0,
                "total_backlinks": 0,
                "domain_authority": 0,
                "avg_position": 0.0,
                "avg_ctr": 0.0,
                "pages": {},
                "keywords": {}
            }
        
        total_organic_traffic = 0
        total_impressions = 0
        total_clicks = 0
        total_backlinks = 0
        domain_authority_sum = 0
        domain_authority_count = 0
        
        pages_data = defaultdict(lambda: {
            "clicks": 0,
            "impressions": 0,
            "ctr": 0.0,
            "position": 0.0
        })
        
        keywords_data = defaultdict(lambda: {
            "clicks": 0,
            "impressions": 0,
            "ctr": 0.0,
            "position": 0.0,
            "count": 0
        })
        
        all_keywords = set()
        top_10_keywords = set()
        
        for record in seo_data:
            # Aggregate basic metrics
            total_organic_traffic += record.get("organic_traffic", 0)
            total_impressions += record.get("impressions", 0)
            total_clicks += record.get("clicks", 0)
            total_backlinks += record.get("backlinks_count", 0)
            
            # Domain authority
            if "domain_authority" in record:
                domain_authority_sum += record["domain_authority"]
                domain_authority_count += 1
            
            # Page-level data
            pages = record.get("pages", [])
            for page in pages:
                page_url = page.get("url", "")
                if page_url:
                    pages_data[page_url]["clicks"] += page.get("clicks", 0)
                    pages_data[page_url]["impressions"] += page.get("impressions", 0)
                    pages_data[page_url]["position"] += page.get("position", 0)
            
            # Keyword-level data
            keywords = record.get("keywords", [])
            for keyword_data in keywords:
                keyword = keyword_data.get("keyword", "")
                if keyword:
                    all_keywords.add(keyword)
                    keywords_data[keyword]["clicks"] += keyword_data.get("clicks", 0)
                    keywords_data[keyword]["impressions"] += keyword_data.get("impressions", 0)
                    keywords_data[keyword]["position"] += keyword_data.get("position", 0)
                    keywords_data[keyword]["count"] += 1
                    
                    # Track top 10 keywords (position <= 10)
                    if keyword_data.get("position", 0) <= 10:
                        top_10_keywords.add(keyword)
        
        # Calculate averages for pages
        for page_url, data in pages_data.items():
            if data["impressions"] > 0:
                data["ctr"] = round((data["clicks"] / data["impressions"]) * 100, 2)
            if data["clicks"] > 0:
                data["position"] = round(data["position"] / data["clicks"], 2)
        
        # Calculate averages for keywords
        for keyword, data in keywords_data.items():
            if data["impressions"] > 0:
                data["ctr"] = round((data["clicks"] / data["impressions"]) * 100, 2)
            if data["count"] > 0:
                data["position"] = round(data["position"] / data["count"], 2)
        
        # Calculate overall averages
        avg_position = 0.0
        avg_ctr = 0.0
        
        if total_clicks > 0:
            position_sum = sum(
                record.get("avg_position", 0) * record.get("clicks", 0)
                for record in seo_data
            )
            avg_position = round(position_sum / total_clicks, 2)
        
        if total_impressions > 0:
            avg_ctr = round((total_clicks / total_impressions) * 100, 2)
        
        domain_authority = 0.0
        if domain_authority_count > 0:
            domain_authority = round(domain_authority_sum / domain_authority_count, 2)
        
        return {
            "total_organic_traffic": total_organic_traffic,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "total_keywords": len(all_keywords),
            "top_10_keywords": len(top_10_keywords),
            "total_backlinks": total_backlinks,
            "domain_authority": domain_authority,
            "avg_position": avg_position,
            "avg_ctr": avg_ctr,
            "pages": dict(pages_data),
            "keywords": dict(keywords_data)
        }
    
    def get_top_pages(
        self,
        seo_data: List[Dict[str, Any]],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top performing pages by clicks
        
        Args:
            seo_data: List of SEO metric documents
            limit: Number of top pages to return
            
        Returns:
            List of top pages with metrics
        """
        pages_dict = defaultdict(lambda: {
            "url": "",
            "clicks": 0,
            "impressions": 0,
            "ctr": 0.0,
            "position": 0.0
        })
        
        for record in seo_data:
            pages = record.get("pages", [])
            for page in pages:
                page_url = page.get("url", "")
                if page_url:
                    if not pages_dict[page_url]["url"]:
                        pages_dict[page_url]["url"] = page_url
                    
                    pages_dict[page_url]["clicks"] += page.get("clicks", 0)
                    pages_dict[page_url]["impressions"] += page.get("impressions", 0)
                    pages_dict[page_url]["position"] += page.get("position", 0)
        
        # Calculate averages
        for page_url, data in pages_dict.items():
            if data["impressions"] > 0:
                data["ctr"] = round((data["clicks"] / data["impressions"]) * 100, 2)
            if data["clicks"] > 0:
                data["position"] = round(data["position"] / data["clicks"], 2)
        
        # Sort by clicks and return top N
        top_pages = sorted(
            pages_dict.values(),
            key=lambda x: x["clicks"],
            reverse=True
        )[:limit]
        
        return top_pages
    
    def get_top_keywords(
        self,
        seo_data: List[Dict[str, Any]],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top performing keywords by clicks
        
        Args:
            seo_data: List of SEO metric documents
            limit: Number of top keywords to return
            
        Returns:
            List of top keywords with metrics
        """
        keywords_dict = defaultdict(lambda: {
            "keyword": "",
            "clicks": 0,
            "impressions": 0,
            "ctr": 0.0,
            "position": 0.0,
            "count": 0
        })
        
        for record in seo_data:
            keywords = record.get("keywords", [])
            for keyword_data in keywords:
                keyword = keyword_data.get("keyword", "")
                if keyword:
                    if not keywords_dict[keyword]["keyword"]:
                        keywords_dict[keyword]["keyword"] = keyword
                    
                    keywords_dict[keyword]["clicks"] += keyword_data.get("clicks", 0)
                    keywords_dict[keyword]["impressions"] += keyword_data.get("impressions", 0)
                    keywords_dict[keyword]["position"] += keyword_data.get("position", 0)
                    keywords_dict[keyword]["count"] += 1
        
        # Calculate averages
        for keyword, data in keywords_dict.items():
            if data["impressions"] > 0:
                data["ctr"] = round((data["clicks"] / data["impressions"]) * 100, 2)
            if data["count"] > 0:
                data["position"] = round(data["position"] / data["count"], 2)
        
        # Sort by clicks and return top N
        top_keywords = sorted(
            keywords_dict.values(),
            key=lambda x: x["clicks"],
            reverse=True
        )[:limit]
        
        return top_keywords


