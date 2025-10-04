#!/usr/bin/env python3
"""
Business Intelligence Layer for Project Sentinel
Provides loss prevention metrics, customer risk scoring, and ROI analysis
"""

import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, Counter


logger = logging.getLogger(__name__)


class LossPreventionMetrics:
    """Calculate and track financial impact of incidents"""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.daily_losses = defaultdict(float)
        self.losses_by_category = defaultdict(float)
        self.losses_by_station = defaultdict(float)
        self.losses_by_customer = defaultdict(float)
    
    def record_loss(self, incident: Dict[str, Any]):
        """Record financial loss from an incident"""
        try:
            loss = incident.get("potential_loss", 0)
            category = incident.get("event_name", "Unknown")
            station_id = incident.get("station_id", "SYSTEM")
            customer_id = incident.get("customer_id")
            
            # Track by date
            date_key = datetime.now().date().isoformat()
            self.daily_losses[date_key] += loss
            
            # Track by category
            self.losses_by_category[category] += loss
            
            # Track by station
            if station_id != "SYSTEM":
                self.losses_by_station[station_id] += loss
            
            # Track by customer
            if customer_id:
                self.losses_by_customer[customer_id] += loss
                
        except Exception as e:
            logger.error(f"Error recording loss: {e}")
    
    def get_daily_loss_summary(self, date: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive loss summary for a day"""
        if date is None:
            date = datetime.now().date().isoformat()
        
        total_loss = self.daily_losses.get(date, 0)
        
        # Get top categories
        top_categories = sorted(
            self.losses_by_category.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        # Get top risk stations
        top_stations = sorted(
            self.losses_by_station.items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]
        
        # Get top risk customers
        top_customers = sorted(
            self.losses_by_customer.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        return {
            "date": date,
            "total_potential_loss": round(total_loss, 2),
            "top_loss_categories": [
                {"category": cat, "loss": round(loss, 2)}
                for cat, loss in top_categories
            ],
            "top_risk_stations": [
                {"station_id": station, "loss": round(loss, 2)}
                for station, loss in top_stations
            ],
            "top_risk_customers": [
                {"customer_id": customer, "loss": round(loss, 2)}
                for customer, loss in top_customers
            ],
            "average_incident_value": round(
                total_loss / max(1, len(self.losses_by_category)),
                2
            )
        }
    
    def get_loss_trends(self, days: int = 7) -> Dict[str, Any]:
        """Analyze loss trends over time"""
        today = datetime.now().date()
        trend_data = []
        
        for i in range(days):
            date = (today - timedelta(days=i)).isoformat()
            loss = self.daily_losses.get(date, 0)
            trend_data.append({"date": date, "loss": round(loss, 2)})
        
        # Calculate trend direction
        if len(trend_data) >= 2:
            recent_avg = np.mean([d["loss"] for d in trend_data[:3]])
            older_avg = np.mean([d["loss"] for d in trend_data[3:]])
            
            if recent_avg > older_avg * 1.2:
                trend = "INCREASING"
            elif recent_avg < older_avg * 0.8:
                trend = "DECREASING"
            else:
                trend = "STABLE"
        else:
            trend = "INSUFFICIENT_DATA"
        
        return {
            "period_days": days,
            "trend_direction": trend,
            "daily_losses": sorted(trend_data, key=lambda x: x["date"]),
            "total_period_loss": round(sum(d["loss"] for d in trend_data), 2),
            "average_daily_loss": round(np.mean([d["loss"] for d in trend_data]), 2)
        }


class CustomerRiskScoring:
    """Calculate and track customer risk scores"""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.risk_thresholds = {
            "LOW": 0,
            "MEDIUM": 30,
            "HIGH": 60,
            "CRITICAL": 80
        }
    
    def calculate_risk_score(self, customer_id: str, days: int = 30) -> float:
        """Calculate comprehensive risk score for a customer"""
        try:
            incidents = self.state_manager.get_customer_incidents(customer_id, days)
            
            if not incidents:
                return 0.0
            
            score = 0.0
            
            # Factor 1: Incident count (0-40 points)
            incident_count = len(incidents)
            score += min(incident_count * 8, 40)
            
            # Factor 2: Severity of incidents (0-30 points)
            severity_map = {
                "Scanner Avoidance": 15,
                "Barcode Switching": 20,
                "Weight Discrepancies": 10,
                "Coordinated Fraud Alert": 30,
                "Unexpected Systems Crash": 5
            }
            
            avg_severity = np.mean([
                severity_map.get(i.get("event_name"), 5)
                for i in incidents
            ])
            score += min(avg_severity, 30)
            
            # Factor 3: Recent activity (0-20 points)
            recent_incidents = [
                i for i in incidents
                if (datetime.now() - i.get("timestamp", datetime.now())).days <= 7
            ]
            score += min(len(recent_incidents) * 5, 20)
            
            # Factor 4: Financial impact (0-10 points)
            total_loss = sum(i.get("potential_loss", 0) for i in incidents)
            if total_loss > 1000:
                score += 10
            elif total_loss > 500:
                score += 7
            elif total_loss > 100:
                score += 4
            
            return min(score, 100.0)
            
        except Exception as e:
            logger.error(f"Error calculating risk score: {e}")
            return 0.0
    
    def get_risk_category(self, score: float) -> str:
        """Convert numerical score to risk category"""
        if score >= self.risk_thresholds["CRITICAL"]:
            return "CRITICAL"
        elif score >= self.risk_thresholds["HIGH"]:
            return "HIGH"
        elif score >= self.risk_thresholds["MEDIUM"]:
            return "MEDIUM"
        else:
            return "LOW"
    
    def get_high_risk_customers(self, min_score: float = 60.0) -> List[Dict[str, Any]]:
        """Get list of high-risk customers"""
        high_risk = []
        
        for customer_id in self.state_manager.customer_incidents.keys():
            score = self.calculate_risk_score(customer_id)
            
            if score >= min_score:
                incidents = self.state_manager.get_customer_incidents(customer_id, 30)
                total_loss = sum(i.get("potential_loss", 0) for i in incidents)
                
                high_risk.append({
                    "customer_id": customer_id,
                    "risk_score": round(score, 1),
                    "risk_category": self.get_risk_category(score),
                    "incident_count": len(incidents),
                    "total_potential_loss": round(total_loss, 2),
                    "last_incident": max(
                        (i.get("timestamp") for i in incidents),
                        default=datetime.now()
                    ).isoformat() if incidents else None,
                    "recommended_action": self._get_recommended_action(score)
                })
        
        return sorted(high_risk, key=lambda x: x["risk_score"], reverse=True)
    
    def _get_recommended_action(self, score: float) -> str:
        """Get recommended action based on risk score"""
        if score >= 80:
            return "IMMEDIATE INTERVENTION - Consider account suspension"
        elif score >= 60:
            return "ENHANCED MONITORING - Flag for manual review"
        elif score >= 30:
            return "WATCH LIST - Increased alert sensitivity"
        else:
            return "STANDARD MONITORING"
    
    def generate_customer_risk_report(self, customer_id: str) -> Dict[str, Any]:
        """Generate detailed risk report for a customer"""
        score = self.calculate_risk_score(customer_id)
        incidents = self.state_manager.get_customer_incidents(customer_id, 30)
        
        # Analyze patterns
        incident_types = Counter(i.get("event_name") for i in incidents)
        stations_used = Counter(i.get("station_id") for i in incidents if i.get("station_id"))
        
        return {
            "customer_id": customer_id,
            "risk_score": round(score, 1),
            "risk_category": self.get_risk_category(score),
            "total_incidents": len(incidents),
            "incident_breakdown": dict(incident_types),
            "preferred_stations": dict(stations_used.most_common(3)),
            "total_potential_loss": round(
                sum(i.get("potential_loss", 0) for i in incidents),
                2
            ),
            "recent_incidents": [
                {
                    "date": i.get("timestamp", datetime.now()).isoformat(),
                    "type": i.get("event_name"),
                    "station": i.get("station_id"),
                    "loss": i.get("potential_loss", 0)
                }
                for i in sorted(incidents, key=lambda x: x.get("timestamp", datetime.now()), reverse=True)[:5]
            ],
            "recommended_action": self._get_recommended_action(score)
        }


class StationHealthMonitor:
    """Monitor and analyze station performance and reliability"""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.health_thresholds = {
            "error_rate": 0.1,  # 10%
            "avg_processing_time": 90,  # seconds
            "rfid_success_rate": 0.9  # 90%
        }
    
    def calculate_station_health(self, station_id: str) -> Dict[str, Any]:
        """Calculate comprehensive health score for a station"""
        try:
            station = self.state_manager.get_station_state(station_id)
            
            if not station:
                return {"station_id": station_id, "status": "NOT_FOUND"}
            
            issues = []
            health_score = 100.0
            
            # Check 1: Error rate
            recent_events = list(station.recent_transactions)[-20:]
            if recent_events:
                error_count = sum(
                    1 for e in recent_events
                    if "error" in str(e.get("status", "")).lower()
                )
                error_rate = error_count / len(recent_events)
                
                if error_rate > self.health_thresholds["error_rate"]:
                    severity = "HIGH" if error_rate > 0.2 else "MEDIUM"
                    health_score -= 30 if severity == "HIGH" else 15
                    issues.append({
                        "type": "High Error Rate",
                        "severity": severity,
                        "value": f"{error_rate*100:.1f}%",
                        "threshold": f"{self.health_thresholds['error_rate']*100:.0f}%",
                        "action": "Technical inspection required"
                    })
            
            # Check 2: Processing speed
            if len(recent_events) >= 2:
                time_diffs = []
                for i in range(len(recent_events) - 1):
                    t1 = recent_events[i].get("timestamp")
                    t2 = recent_events[i+1].get("timestamp")
                    if t1 and t2:
                        diff = (t2 - t1).total_seconds()
                        if diff > 0:
                            time_diffs.append(diff)
                
                if time_diffs:
                    avg_processing = np.mean(time_diffs)
                    
                    if avg_processing > self.health_thresholds["avg_processing_time"]:
                        health_score -= 20
                        issues.append({
                            "type": "Slow Processing",
                            "severity": "MEDIUM",
                            "value": f"{avg_processing:.0f} seconds/transaction",
                            "threshold": f"{self.health_thresholds['avg_processing_time']} seconds",
                            "action": "Performance optimization needed"
                        })
            
            # Check 3: RFID reader health
            rfid_reads = list(station.recent_rfid_reads)
            if rfid_reads:
                successful_reads = sum(
                    1 for r in rfid_reads
                    if r.get("status") == "Active"
                )
                rfid_success_rate = successful_reads / len(rfid_reads)
                
                if rfid_success_rate < self.health_thresholds["rfid_success_rate"]:
                    health_score -= 25
                    issues.append({
                        "type": "RFID Reader Issues",
                        "severity": "HIGH",
                        "value": f"{rfid_success_rate*100:.0f}% success rate",
                        "threshold": f"{self.health_thresholds['rfid_success_rate']*100:.0f}%",
                        "action": "RFID reader maintenance required"
                    })
            
            # Check 4: Queue buildup
            if station.customer_count > 8:
                health_score -= 10
                issues.append({
                    "type": "Queue Buildup",
                    "severity": "MEDIUM",
                    "value": f"{station.customer_count} customers",
                    "threshold": "8 customers",
                    "action": "Consider opening additional lanes"
                })
            
            # Determine overall health
            if health_score >= 80:
                overall_health = "EXCELLENT"
            elif health_score >= 60:
                overall_health = "GOOD"
            elif health_score >= 40:
                overall_health = "DEGRADED"
            else:
                overall_health = "CRITICAL"
            
            return {
                "station_id": station_id,
                "health_score": round(health_score, 1),
                "overall_health": overall_health,
                "issues": issues,
                "requires_maintenance": overall_health in ["DEGRADED", "CRITICAL"],
                "last_update": station.last_update.isoformat() if station.last_update else None,
                "uptime_indicators": {
                    "error_rate": f"{error_rate*100:.1f}%" if recent_events else "N/A",
                    "avg_processing_time": f"{avg_processing:.0f}s" if len(recent_events) >= 2 else "N/A",
                    "current_queue": station.customer_count
                }
            }
            
        except Exception as e:
            logger.error(f"Error calculating station health: {e}")
            return {"station_id": station_id, "status": "ERROR", "error": str(e)}
    
    def get_all_stations_health(self) -> List[Dict[str, Any]]:
        """Get health reports for all stations"""
        health_reports = []
        
        for station_id in self.state_manager.stations.keys():
            health = self.calculate_station_health(station_id)
            health_reports.append(health)
        
        return sorted(health_reports, key=lambda x: x.get("health_score", 0))


class TrendAnalyzer:
    """Analyze trends and patterns in store operations"""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.baseline_window = timedelta(hours=1)
    
    def detect_anomalous_trends(self) -> List[Dict[str, Any]]:
        """Detect abnormal trends in real-time metrics"""
        alerts = []
        
        try:
            current_metrics = self._get_current_metrics()
            baseline_metrics = self._get_baseline_metrics()
            
            thresholds = {
                "incident_rate": 2.0,
                "queue_length": 1.5,
                "wait_time": 1.8,
                "error_rate": 2.5
            }
            
            for metric, current_value in current_metrics.items():
                baseline_value = baseline_metrics.get(metric, current_value)
                
                if baseline_value > 0:
                    ratio = current_value / baseline_value
                    threshold = thresholds.get(metric, 2.0)
                    
                    if ratio > threshold:
                        increase_pct = ((ratio - 1) * 100)
                        
                        alerts.append({
                            "event_name": f"Trend Alert - {metric.replace('_', ' ').title()}",
                            "metric": metric,
                            "current_value": round(current_value, 2),
                            "baseline_value": round(baseline_value, 2),
                            "increase_percentage": round(increase_pct, 1),
                            "severity": "HIGH" if ratio > threshold * 1.5 else "MEDIUM",
                            "recommended_action": self._get_trend_recommendation(metric, ratio)
                        })
            
        except Exception as e:
            logger.error(f"Error detecting trends: {e}")
        
        return alerts
    
    def _get_current_metrics(self) -> Dict[str, float]:
        """Get current operational metrics"""
        stats = self.state_manager.get_system_stats()
        
        return {
            "queue_length": stats.get("total_customers_in_queue", 0),
            "wait_time": stats.get("average_wait_time", 0),
            "incident_rate": len(self.state_manager.get_recent_incidents(minutes=15)),
            "error_rate": self.state_manager.get_system_error_rate()
        }
    
    def _get_baseline_metrics(self) -> Dict[str, float]:
        """Get baseline metrics from historical data"""
        # Simplified: In production, use actual historical averages
        stats = self.state_manager.get_system_stats()
        
        return {
            "queue_length": 3.0,
            "wait_time": 120.0,
            "incident_rate": 2.0,
            "error_rate": 0.05
        }
    
    def _get_trend_recommendation(self, metric: str, ratio: float) -> str:
        """Get recommendation based on trend"""
        recommendations = {
            "incident_rate": "Increase security monitoring and staff oversight",
            "queue_length": "Open additional checkout lanes immediately",
            "wait_time": "Allocate more staff to assist customers",
            "error_rate": "Investigate system issues and perform diagnostics"
        }
        
        return recommendations.get(metric, "Monitor situation closely")


class BusinessIntelligenceDashboard:
    """Main BI dashboard aggregating all metrics"""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.loss_metrics = LossPreventionMetrics(state_manager)
        self.risk_scoring = CustomerRiskScoring(state_manager)
        self.station_health = StationHealthMonitor(state_manager)
        self.trend_analyzer = TrendAnalyzer(state_manager)
    
    def get_executive_summary(self) -> Dict[str, Any]:
        """Get high-level executive summary"""
        loss_summary = self.loss_metrics.get_daily_loss_summary()
        high_risk_customers = self.risk_scoring.get_high_risk_customers(min_score=60)
        station_health = self.station_health.get_all_stations_health()
        trends = self.trend_analyzer.detect_anomalous_trends()
        
        # Calculate KPIs
        critical_stations = [s for s in station_health if s.get("overall_health") == "CRITICAL"]
        total_potential_loss = loss_summary.get("total_potential_loss", 0)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "kpis": {
                "total_potential_loss_today": round(total_potential_loss, 2),
                "high_risk_customers": len(high_risk_customers),
                "critical_stations": len(critical_stations),
                "active_trend_alerts": len(trends)
            },
            "top_priorities": self._identify_top_priorities(
                loss_summary, high_risk_customers, critical_stations, trends
            ),
            "loss_summary": loss_summary,
            "high_risk_customers": high_risk_customers[:5],
            "station_health_overview": {
                "total_stations": len(station_health),
                "critical_stations": len(critical_stations),
                "avg_health_score": round(
                    np.mean([s.get("health_score", 0) for s in station_health]),
                    1
                )
            },
            "trend_alerts": trends
        }
    
    def _identify_top_priorities(self, loss_summary, high_risk_customers, 
                                 critical_stations, trends) -> List[str]:
        """Identify top 3 priorities for management"""
        priorities = []
        
        if loss_summary.get("total_potential_loss", 0) > 500:
            priorities.append(
                f"HIGH LOSS ALERT: ${loss_summary['total_potential_loss']:.0f} in potential losses today"
            )
        
        if len(high_risk_customers) > 5:
            priorities.append(
                f"SECURITY CONCERN: {len(high_risk_customers)} high-risk customers active"
            )
        
        if len(critical_stations) > 0:
            priorities.append(
                f"MAINTENANCE REQUIRED: {len(critical_stations)} stations in critical condition"
            )
        
        if len(trends) > 0:
            priorities.append(
                f"OPERATIONAL ALERT: {len(trends)} abnormal trends detected"
            )
        
        return priorities[:3]
    