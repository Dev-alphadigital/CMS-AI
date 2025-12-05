"""
Sentiment Analyzer - Analyze brand sentiment from social media and mentions
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from .llm_client import LLMClient
import re

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """
    Analyze sentiment of text using AI models
    """
    
    def __init__(self, provider: str = "openai"):
        self.llm = LLMClient(provider=provider)
    
    async def analyze_text(self, text: str) -> Dict[str, Any]:
        """
        Analyze sentiment of a single text
        
        Args:
            text: Text to analyze
            
        Returns:
            {
                "sentiment": "positive" | "negative" | "neutral",
                "score": -1.0 to 1.0,
                "confidence": 0.0 to 1.0,
                "keywords": ["keyword1", "keyword2"],
                "emotions": {"joy": 0.8, "anger": 0.1, ...}
            }
        """
        try:
            # Quick validation
            if not text or len(text.strip()) < 3:
                return {
                    "sentiment": "neutral",
                    "score": 0.0,
                    "confidence": 0.0,
                    "keywords": [],
                    "emotions": {}
                }
            
            # Use LLM for analysis
            result = await self.llm.analyze_sentiment(text)
            
            # Extract keywords (simple approach - can be enhanced)
            keywords = self._extract_keywords(text)
            
            # Detect emotions using LLM
            emotions = await self._detect_emotions(text)
            
            return {
                "sentiment": result.get("sentiment", "neutral"),
                "score": result.get("score", 0.0),
                "confidence": result.get("confidence", 0.0),
                "keywords": keywords,
                "emotions": emotions,
                "text_length": len(text),
                "analyzed_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Sentiment analysis failed: {str(e)}")
            # Return neutral sentiment on error
            return {
                "sentiment": "neutral",
                "score": 0.0,
                "confidence": 0.0,
                "keywords": [],
                "emotions": {},
                "error": str(e)
            }
    
    async def analyze_batch(
        self,
        texts: List[str],
        batch_size: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Analyze sentiment for multiple texts
        
        Args:
            texts: List of texts to analyze
            batch_size: Process in batches to avoid rate limits
            
        Returns:
            List of sentiment analysis results
        """
        results = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            for text in batch:
                result = await self.analyze_text(text)
                results.append(result)
        
        return results
    
    async def analyze_brand_mentions(
        self,
        mentions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Analyze sentiment across multiple brand mentions
        
        Args:
            mentions: List of brand mentions with 'content' field
            
        Returns:
            Aggregate sentiment analysis
        """
        try:
            if not mentions:
                return {
                    "overall_sentiment": "neutral",
                    "overall_score": 0.0,
                    "sentiment_distribution": {
                        "positive": 0,
                        "neutral": 0,
                        "negative": 0
                    },
                    "total_mentions": 0
                }
            
            # Analyze each mention
            sentiment_results = []
            for mention in mentions:
                content = mention.get("content", "")
                if content:
                    result = await self.analyze_text(content)
                    sentiment_results.append(result)
            
            # Calculate aggregates
            positive_count = sum(1 for r in sentiment_results if r["sentiment"] == "positive")
            negative_count = sum(1 for r in sentiment_results if r["sentiment"] == "negative")
            neutral_count = sum(1 for r in sentiment_results if r["sentiment"] == "neutral")
            
            # Calculate overall score (average)
            scores = [r["score"] for r in sentiment_results if "score" in r]
            overall_score = sum(scores) / len(scores) if scores else 0.0
            
            # Determine overall sentiment
            if overall_score > 0.2:
                overall_sentiment = "positive"
            elif overall_score < -0.2:
                overall_sentiment = "negative"
            else:
                overall_sentiment = "neutral"
            
            # Extract common keywords
            all_keywords = []
            for r in sentiment_results:
                all_keywords.extend(r.get("keywords", []))
            
            keyword_counts = {}
            for keyword in all_keywords:
                keyword_counts[keyword] = keyword_counts.get(keyword, 0) + 1
            
            # Sort by frequency
            top_keywords = sorted(
                keyword_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
            
            # Aggregate emotions
            emotion_totals = {}
            for r in sentiment_results:
                emotions = r.get("emotions", {})
                for emotion, score in emotions.items():
                    emotion_totals[emotion] = emotion_totals.get(emotion, 0) + score
            
            # Average emotions
            avg_emotions = {
                emotion: score / len(sentiment_results)
                for emotion, score in emotion_totals.items()
            }
            
            return {
                "overall_sentiment": overall_sentiment,
                "overall_score": round(overall_score, 3),
                "confidence": round(sum(r.get("confidence", 0) for r in sentiment_results) / len(sentiment_results), 2),
                "sentiment_distribution": {
                    "positive": positive_count,
                    "neutral": neutral_count,
                    "negative": negative_count
                },
                "sentiment_percentages": {
                    "positive": round(positive_count / len(sentiment_results) * 100, 1),
                    "neutral": round(neutral_count / len(sentiment_results) * 100, 1),
                    "negative": round(negative_count / len(sentiment_results) * 100, 1)
                },
                "total_mentions": len(mentions),
                "analyzed_mentions": len(sentiment_results),
                "top_keywords": [{"keyword": k, "count": c} for k, c in top_keywords],
                "emotions": avg_emotions,
                "analyzed_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Brand mention analysis failed: {str(e)}")
            return {
                "overall_sentiment": "neutral",
                "overall_score": 0.0,
                "total_mentions": len(mentions),
                "error": str(e)
            }
    
    async def detect_trends(
        self,
        historical_sentiments: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Detect sentiment trends over time
        
        Args:
            historical_sentiments: List of sentiment data with dates
            
        Returns:
            Trend analysis
        """
        try:
            if len(historical_sentiments) < 2:
                return {
                    "trend": "insufficient_data",
                    "direction": "stable",
                    "change": 0.0
                }
            
            # Sort by date
            sorted_data = sorted(
                historical_sentiments,
                key=lambda x: x.get("date", "")
            )
            
            # Calculate moving average
            scores = [d.get("sentiment_score", 0) for d in sorted_data]
            
            # Compare recent vs older periods
            if len(scores) >= 7:
                recent_avg = sum(scores[-7:]) / 7
                older_avg = sum(scores[-14:-7]) / 7 if len(scores) >= 14 else sum(scores[:-7]) / len(scores[:-7])
            else:
                recent_avg = sum(scores[-3:]) / min(3, len(scores))
                older_avg = sum(scores[:-3]) / len(scores[:-3]) if len(scores) > 3 else recent_avg
            
            change = recent_avg - older_avg
            
            # Determine trend
            if change > 0.1:
                trend = "improving"
                direction = "up"
            elif change < -0.1:
                trend = "declining"
                direction = "down"
            else:
                trend = "stable"
                direction = "stable"
            
            # Identify issues if declining
            issues = []
            if trend == "declining":
                # Find negative spikes
                for i, sentiment_data in enumerate(sorted_data[-7:]):
                    if sentiment_data.get("sentiment_score", 0) < -0.3:
                        issues.append({
                            "date": sentiment_data.get("date"),
                            "score": sentiment_data.get("sentiment_score"),
                            "keywords": sentiment_data.get("top_keywords", [])[:3]
                        })
            
            return {
                "trend": trend,
                "direction": direction,
                "change": round(change, 3),
                "recent_average": round(recent_avg, 3),
                "previous_average": round(older_avg, 3),
                "data_points": len(historical_sentiments),
                "issues": issues if issues else None,
                "recommendation": self._get_trend_recommendation(trend, change)
            }
            
        except Exception as e:
            logger.error(f"Trend detection failed: {str(e)}")
            return {
                "trend": "error",
                "error": str(e)
            }
    
    async def compare_competitors(
        self,
        brand_sentiment: Dict[str, Any],
        competitor_sentiments: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Compare brand sentiment with competitors
        
        Args:
            brand_sentiment: Your brand's sentiment data
            competitor_sentiments: List of competitor sentiment data
            
        Returns:
            Comparative analysis
        """
        try:
            brand_score = brand_sentiment.get("overall_score", 0.0)
            
            comparisons = []
            for comp in competitor_sentiments:
                comp_score = comp.get("overall_score", 0.0)
                difference = brand_score - comp_score
                
                comparisons.append({
                    "competitor": comp.get("name", "Unknown"),
                    "competitor_score": round(comp_score, 3),
                    "difference": round(difference, 3),
                    "performance": "better" if difference > 0 else "worse" if difference < 0 else "similar"
                })
            
            # Sort by competitor score
            comparisons.sort(key=lambda x: x["competitor_score"], reverse=True)
            
            # Calculate ranking
            all_scores = [brand_score] + [c["competitor_score"] for c in comparisons]
            all_scores.sort(reverse=True)
            brand_rank = all_scores.index(brand_score) + 1
            
            # Average competitor score
            avg_competitor_score = sum(c["competitor_score"] for c in comparisons) / len(comparisons) if comparisons else 0
            
            return {
                "brand_score": round(brand_score, 3),
                "brand_rank": brand_rank,
                "total_brands": len(all_scores),
                "avg_competitor_score": round(avg_competitor_score, 3),
                "vs_average": round(brand_score - avg_competitor_score, 3),
                "comparisons": comparisons,
                "insights": self._generate_competitive_insights(brand_score, comparisons)
            }
            
        except Exception as e:
            logger.error(f"Competitor comparison failed: {str(e)}")
            return {"error": str(e)}
    
    async def _detect_emotions(self, text: str) -> Dict[str, float]:
        """
        Detect emotions in text using LLM
        
        Returns:
            Dictionary of emotions with scores (0.0 to 1.0)
        """
        try:
            prompt = f"""Analyze the emotions in this text and rate each emotion from 0.0 to 1.0:

Text: "{text}"

Respond with ONLY valid JSON:
{{
    "joy": 0.0,
    "sadness": 0.0,
    "anger": 0.0,
    "fear": 0.0,
    "surprise": 0.0,
    "trust": 0.0
}}"""
            
            schema = {
                "joy": "number",
                "sadness": "number",
                "anger": "number",
                "fear": "number",
                "surprise": "number",
                "trust": "number"
            }
            
            result = await self.llm.generate_structured_output(prompt, schema)
            return result
            
        except Exception as e:
            logger.error(f"Emotion detection failed: {str(e)}")
            return {}
    
    def _extract_keywords(self, text: str, max_keywords: int = 5) -> List[str]:
        """
        Extract keywords from text (simple implementation)
        
        For production, consider using:
        - spaCy for NLP
        - RAKE algorithm
        - TF-IDF
        """
        # Clean text
        text = text.lower()
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Remove common stop words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has',
            'had', 'do', 'does', 'did', 'will', 'would', 'should', 'could', 'may',
            'might', 'must', 'can', 'this', 'that', 'these', 'those', 'i', 'you',
            'he', 'she', 'it', 'we', 'they', 'what', 'which', 'who', 'when', 'where',
            'why', 'how', 'with', 'from', 'by', 'about', 'into', 'through', 'during',
            'before', 'after', 'above', 'below', 'between', 'under', 'again', 'further',
            'then', 'once', 'here', 'there', 'all', 'both', 'each', 'few', 'more',
            'most', 'other', 'some', 'such', 'only', 'own', 'same', 'so', 'than',
            'too', 'very', 'just', 'now'
        }
        
        # Split into words
        words = text.split()
        
        # Filter and count
        word_counts = {}
        for word in words:
            if len(word) > 3 and word not in stop_words:
                word_counts[word] = word_counts.get(word, 0) + 1
        
        # Sort by frequency
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        
        # Return top keywords
        return [word for word, count in sorted_words[:max_keywords]]
    
    def _get_trend_recommendation(self, trend: str, change: float) -> str:
        """Generate recommendation based on trend"""
        if trend == "improving":
            return "Sentiment is improving. Continue current engagement strategies."
        elif trend == "declining":
            if change < -0.3:
                return "Urgent: Sentiment declining significantly. Review recent content and address customer concerns immediately."
            else:
                return "Sentiment declining. Monitor closely and consider adjusting communication strategy."
        else:
            return "Sentiment stable. Maintain current approach while looking for growth opportunities."
    
    def _generate_competitive_insights(
        self,
        brand_score: float,
        comparisons: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate insights from competitive analysis"""
        insights = []
        
        # Overall performance
        better_count = sum(1 for c in comparisons if c["performance"] == "better")
        worse_count = sum(1 for c in comparisons if c["performance"] == "worse")
        
        if better_count > worse_count:
            insights.append(
                f"Your brand sentiment is better than {better_count} out of {len(comparisons)} competitors."
            )
        elif worse_count > better_count:
            insights.append(
                f"Your brand sentiment is lower than {worse_count} out of {len(comparisons)} competitors. "
                "Consider analyzing top performers' strategies."
            )
        
        # Identify leaders
        top_competitor = comparisons[0] if comparisons else None
        if top_competitor and top_competitor["performance"] == "worse":
            insights.append(
                f"{top_competitor['competitor']} leads with a score of {top_competitor['competitor_score']}. "
                "Study their engagement approach."
            )
        
        # Score-based insights
        if brand_score > 0.5:
            insights.append("Strong positive sentiment. Leverage this in marketing materials.")
        elif brand_score < -0.3:
            insights.append("Negative sentiment detected. Immediate reputation management recommended.")
        
        return insights
    
    async def close(self):
        """Close LLM client"""
        await self.llm.close()