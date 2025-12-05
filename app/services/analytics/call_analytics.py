"""
Call Analytics - Calculate cold calling metrics and insights
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class CallAnalytics:
    """
    Analytics engine for cold calling metrics calculation
    """
    
    def calculate_overview_metrics(self, call_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate overview metrics from call data
        
        Args:
            call_data: List of call documents
            
        Returns:
            Dict with calculated overview metrics
        """
        if not call_data:
            return {
                "total_calls": 0,
                "total_duration": 0,
                "avg_duration": 0.0,
                "success_rate": 0.0,
                "outcome_breakdown": {}
            }
        
        total_calls = len(call_data)
        total_duration = sum(c.get("duration", 0) for c in call_data)
        avg_duration = total_duration / total_calls if total_calls > 0 else 0.0
        
        # Count outcomes
        outcome_counts = {}
        for call in call_data:
            outcome = call.get("outcome", "unknown")
            outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        
        # Calculate success rate (interested + callback as success)
        successful_calls = outcome_counts.get("interested", 0) + outcome_counts.get("callback", 0)
        success_rate = (successful_calls / total_calls * 100) if total_calls > 0 else 0.0
        
        return {
            "total_calls": total_calls,
            "total_duration": total_duration,
            "total_duration_minutes": round(total_duration / 60, 2),
            "avg_duration": round(avg_duration, 2),
            "avg_duration_minutes": round(avg_duration / 60, 2),
            "success_rate": round(success_rate, 2),
            "outcome_breakdown": outcome_counts
        }
    
    def calculate_trends(
        self,
        current_metrics: Dict[str, Any],
        previous_metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate percentage change between current and previous period
        
        Args:
            current_metrics: Metrics for current period
            previous_metrics: Metrics for previous period
            
        Returns:
            Dict with percentage changes for key metrics
        """
        def calc_change(current: float, previous: float) -> float:
            """Calculate percentage change between two values"""
            if previous == 0:
                return 100.0 if current > 0 else 0.0
            return round(((current - previous) / previous) * 100, 2)
        
        return {
            "calls_change": calc_change(
                current_metrics.get("total_calls", 0),
                previous_metrics.get("total_calls", 0)
            ),
            "duration_change": calc_change(
                current_metrics.get("total_duration", 0),
                previous_metrics.get("total_duration", 0)
            ),
            "success_rate_change": calc_change(
                current_metrics.get("success_rate", 0),
                previous_metrics.get("success_rate", 0)
            ),
            "avg_duration_change": calc_change(
                current_metrics.get("avg_duration", 0),
                previous_metrics.get("avg_duration", 0)
            )
        }
    
    def analyze_agent_performance(
        self,
        call_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Analyze performance by agent
        
        Args:
            call_data: List of call documents
            
        Returns:
            List of agent performance metrics sorted by success rate
        """
        agent_stats = {}
        
        for call in call_data:
            agent_id = call.get("agent_id")
            agent_name = call.get("agent_name", "Unknown")
            
            if agent_id not in agent_stats:
                agent_stats[agent_id] = {
                    "agent_id": agent_id,
                    "agent_name": agent_name,
                    "total_calls": 0,
                    "total_duration": 0,
                    "outcomes": {}
                }
            
            stats = agent_stats[agent_id]
            stats["total_calls"] += 1
            stats["total_duration"] += call.get("duration", 0)
            
            outcome = call.get("outcome", "unknown")
            stats["outcomes"][outcome] = stats["outcomes"].get(outcome, 0) + 1
        
        # Calculate metrics for each agent
        agent_performance = []
        for agent_id, stats in agent_stats.items():
            total_calls = stats["total_calls"]
            successful = stats["outcomes"].get("interested", 0) + stats["outcomes"].get("callback", 0)
            success_rate = (successful / total_calls * 100) if total_calls > 0 else 0.0
            
            agent_performance.append({
                "agent_id": agent_id,
                "agent_name": stats["agent_name"],
                "total_calls": total_calls,
                "total_duration_minutes": round(stats["total_duration"] / 60, 2),
                "avg_duration_seconds": round(stats["total_duration"] / total_calls, 2) if total_calls > 0 else 0,
                "success_rate": round(success_rate, 2),
                "outcome_breakdown": stats["outcomes"]
            })
        
        # Sort by success rate (descending)
        return sorted(agent_performance, key=lambda x: x["success_rate"], reverse=True)
    
    def analyze_hourly_performance(
        self,
        call_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Analyze call performance by hour of day
        
        Args:
            call_data: List of call documents with called_at timestamps
            
        Returns:
            List of hourly performance metrics
        """
        hourly_stats = {}
        
        for call in call_data:
            called_at = call.get("called_at")
            if not called_at:
                continue
            
            # Handle both datetime objects and ISO strings
            if isinstance(called_at, str):
                try:
                    called_at = datetime.fromisoformat(called_at.replace('Z', '+00:00'))
                except:
                    continue
            
            if not isinstance(called_at, datetime):
                continue
            
            hour = called_at.hour
            
            if hour not in hourly_stats:
                hourly_stats[hour] = {
                    "hour": hour,
                    "total_calls": 0,
                    "successful_calls": 0,
                    "total_duration": 0
                }
            
            stats = hourly_stats[hour]
            stats["total_calls"] += 1
            stats["total_duration"] += call.get("duration", 0)
            
            outcome = call.get("outcome", "")
            if outcome in ["interested", "callback"]:
                stats["successful_calls"] += 1
        
        # Calculate success rates
        result = []
        for hour, stats in hourly_stats.items():
            success_rate = (
                (stats["successful_calls"] / stats["total_calls"] * 100)
                if stats["total_calls"] > 0 else 0.0
            )
            
            result.append({
                "hour": hour,
                "total_calls": stats["total_calls"],
                "successful_calls": stats["successful_calls"],
                "success_rate": round(success_rate, 2),
                "avg_duration_seconds": round(
                    stats["total_duration"] / stats["total_calls"], 2
                ) if stats["total_calls"] > 0 else 0
            })
        
        # Sort by hour
        return sorted(result, key=lambda x: x["hour"])
    
    def analyze_outcome_patterns(
        self,
        call_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Analyze patterns in call outcomes
        
        Args:
            call_data: List of call documents
            
        Returns:
            Analysis of outcome patterns
        """
        outcome_analysis = {
            "total_calls": len(call_data),
            "outcome_distribution": {},
            "success_rate": 0.0,
            "conversion_rate": 0.0,
            "contact_rate": 0.0
        }
        
        if not call_data:
            return outcome_analysis
        
        # Count outcomes
        outcome_counts = {}
        for call in call_data:
            outcome = call.get("outcome", "unknown")
            outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        
        total_calls = len(call_data)
        successful = outcome_counts.get("interested", 0) + outcome_counts.get("callback", 0)
        contacted = total_calls - outcome_counts.get("no_answer", 0) - outcome_counts.get("voicemail", 0)
        interested = outcome_counts.get("interested", 0)
        
        outcome_analysis["outcome_distribution"] = {
            outcome: {
                "count": count,
                "percentage": round((count / total_calls * 100), 2)
            }
            for outcome, count in outcome_counts.items()
        }
        
        outcome_analysis["success_rate"] = round(
            (successful / total_calls * 100) if total_calls > 0 else 0, 2
        )
        
        outcome_analysis["conversion_rate"] = round(
            (interested / total_calls * 100) if total_calls > 0 else 0, 2
        )
        
        outcome_analysis["contact_rate"] = round(
            (contacted / total_calls * 100) if total_calls > 0 else 0, 2
        )
        
        return outcome_analysis
    
    def calculate_call_efficiency(
        self,
        call_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Calculate call efficiency metrics
        
        Args:
            call_data: List of call documents
            
        Returns:
            Efficiency metrics
        """
        if not call_data:
            return {
                "calls_per_hour": 0.0,
                "successful_calls_per_hour": 0.0,
                "avg_time_to_success": 0.0,
                "efficiency_score": 0.0
            }
        
        total_calls = len(call_data)
        total_duration = sum(c.get("duration", 0) for c in call_data)
        successful_calls = sum(
            1 for c in call_data
            if c.get("outcome") in ["interested", "callback"]
        )
        
        total_hours = total_duration / 3600  # Convert seconds to hours
        
        calls_per_hour = total_calls / total_hours if total_hours > 0 else 0.0
        successful_calls_per_hour = successful_calls / total_hours if total_hours > 0 else 0.0
        
        # Calculate average time to success (only for successful calls)
        successful_durations = [
            c.get("duration", 0) for c in call_data
            if c.get("outcome") in ["interested", "callback"]
        ]
        avg_time_to_success = (
            sum(successful_durations) / len(successful_durations)
            if successful_durations else 0.0
        )
        
        # Efficiency score (0-100): combination of calls/hour and success rate
        success_rate = (successful_calls / total_calls * 100) if total_calls > 0 else 0.0
        efficiency_score = min(
            (calls_per_hour * 0.5 + success_rate * 0.5),
            100.0
        )
        
        return {
            "calls_per_hour": round(calls_per_hour, 2),
            "successful_calls_per_hour": round(successful_calls_per_hour, 2),
            "avg_time_to_success_seconds": round(avg_time_to_success, 2),
            "avg_time_to_success_minutes": round(avg_time_to_success / 60, 2),
            "efficiency_score": round(efficiency_score, 2)
        }
    
    def get_best_calling_times(
        self,
        call_data: List[Dict[str, Any]],
        min_calls: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Identify best times to make calls based on success rates
        
        Args:
            call_data: List of call documents
            min_calls: Minimum number of calls required for a time slot to be considered
            
        Returns:
            List of time slots sorted by success rate
        """
        hourly_performance = self.analyze_hourly_performance(call_data)
        
        # Filter by minimum calls and sort by success rate
        best_times = [
            time_slot for time_slot in hourly_performance
            if time_slot["total_calls"] >= min_calls
        ]
        
        return sorted(best_times, key=lambda x: x["success_rate"], reverse=True)


