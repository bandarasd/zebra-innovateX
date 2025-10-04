#!/usr/bin/env python3
"""
Predictive Analytics for Project Sentinel
Forecasts staffing needs, rush hours, and operational requirements
"""

import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, deque


logger = logging.getLogger(__name__)


class PredictiveStaffing:
    """Predict staffing needs before rush hits"""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.historical_patterns = defaultdict(lambda: defaultdict(list))
        self.arrival_rate_window = deque(maxlen=10)  # Last 10 measurements
        self.forecast_window = 30  # minutes
    
    def record_current_state(self):
        """Record current state for learning"""
        try:
            stats = self.state_manager.get_system_stats()
            now = datetime.now()
            
            # Store by day of week and hour
            day_of_week = now.weekday()  # 0=Monday, 6=Sunday
            hour = now.hour
            
            customer_count = stats.get("total_customers_in_queue", 0)
            
            self.historical_patterns[day_of_week][hour].append({
                "timestamp": now,
                "customer_count": customer_count,
                "active_stations": stats.get("active_stations", 0)
            })
            
            # Track arrival rate
            if len(self.arrival_rate_window) > 0:
                last_count = self.arrival_rate_window[-1]["customer_count"]
                time_diff = (now - self.arrival_rate_window[-1]["timestamp"]).total_seconds() / 60
                
                if time_diff > 0:
                    arrival_rate = (customer_count - last_count) / time_diff
                    self.arrival_rate_window.append({
                        "timestamp": now,
                        "customer_count": customer_count,
                        "arrival_rate": arrival_rate
                    })
            else:
                self.arrival_rate_window.append({
                    "timestamp": now,
                    "customer_count": customer_count,
                    "arrival_rate": 0
                })
                
        except Exception as e:
            logger.error(f"Error recording state: {e}")
    
    def predict_next_30_minutes(self) -> Optional[Dict[str, Any]]:
        """Predict customer volume and staffing needs for next 30 minutes"""
        try:
            now = datetime.now()
            current_stats = self.state_manager.get_system_stats()
            current_customers = current_stats.get("total_customers_in_queue", 0)
            
            # Get historical average for this time
            historical_avg = self._get_historical_average(now)
            
            # Get current trend
            current_trend = self._calculate_current_trend()
            
            # Weighted prediction
            if historical_avg is not None and current_trend is not None:
                # 40% historical, 60% current trend
                predicted_customers = (
                    historical_avg * 0.4 +
                    (current_customers + current_trend) * 0.6
                )
            elif historical_avg is not None:
                predicted_customers = historical_avg * 1.2
            else:
                predicted_customers = current_customers * 1.5
            
            predicted_customers = max(0, int(predicted_customers))
            
            # Calculate recommended staffing
            recommended_stations = max(1, int(np.ceil(predicted_customers / 6)))
            current_stations = current_stats.get("active_stations", 1)
            additional_needed = max(0, recommended_stations - current_stations)
            
            # Determine urgency
            if predicted_customers >= 20:
                urgency = "CRITICAL"
                action_time = "IMMEDIATE"
            elif predicted_customers >= 15:
                urgency = "HIGH"
                action_time = "Within 5 minutes"
            elif predicted_customers >= 10:
                urgency = "MEDIUM"
                action_time = "Within 15 minutes"
            else:
                urgency = "LOW"
                action_time = "Monitor"
            
            confidence = self._calculate_prediction_confidence()
            
            if additional_needed > 0 or urgency != "LOW":
                return {
                    "event_name": "Predictive Staffing Alert",
                    "prediction_horizon": f"{self.forecast_window} minutes",
                    "current_customers": current_customers,
                    "predicted_customers": predicted_customers,
                    "current_stations": current_stations,
                    "recommended_stations": recommended_stations,
                    "additional_stations_needed": additional_needed,
                    "urgency": urgency,
                    "action_timeline": action_time,
                    "confidence": round(confidence, 2),
                    "reasoning": self._generate_reasoning(
                        historical_avg, current_trend, predicted_customers
                    )
                }
            
        except Exception as e:
            logger.error(f"Error predicting staffing needs: {e}")
        
        return None
    
    def _get_historical_average(self, target_time: datetime) -> Optional[float]:
        """Get historical average for a specific time"""
        day_of_week = target_time.weekday()
        hour = target_time.hour
        
        historical_data = self.historical_patterns.get(day_of_week, {}).get(hour, [])
        
        if len(historical_data) >= 3:
            # Use recent history (last 4 weeks)
            recent_data = historical_data[-12:]  # Assuming hourly records
            avg = np.mean([d["customer_count"] for d in recent_data])
            return avg
        
        return None
    
    def _calculate_current_trend(self) -> Optional[float]:
        """Calculate current arrival trend"""
        if len(self.arrival_rate_window) < 3:
            return None
        
        recent_rates = [
            d["arrival_rate"] for d in list(self.arrival_rate_window)[-3:]
        ]
        
        avg_rate = np.mean(recent_rates)
        
        # Project forward 30 minutes
        return avg_rate * self.forecast_window
    
    def _calculate_prediction_confidence(self) -> float:
        """Calculate confidence in prediction"""
        confidence = 0.5  # Base confidence
        
        # Increase confidence with more historical data
        day_of_week = datetime.now().weekday()
        hour = datetime.now().hour
        historical_data = self.historical_patterns.get(day_of_week, {}).get(hour, [])
        
        if len(historical_data) >= 10:
            confidence += 0.3
        elif len(historical_data) >= 5:
            confidence += 0.2
        elif len(historical_data) >= 2:
            confidence += 0.1
        
        # Increase confidence with current trend data
        if len(self.arrival_rate_window) >= 5:
            confidence += 0.2
        
        return min(confidence, 0.95)
    
    def _generate_reasoning(self, historical_avg, current_trend, prediction) -> str:
        """Generate human-readable reasoning"""
        reasons = []
        
        if historical_avg is not None:
            reasons.append(f"Historical average: {historical_avg:.0f} customers")
        
        if current_trend is not None:
            reasons.append(f"Current trend: +{current_trend:.0f} customers/30min")
        
        reasons.append(f"Predicted total: {prediction:.0f} customers")
        
        return " | ".join(reasons)
    
    def predict_rush_hours(self) -> List[Dict[str, Any]]:
        """Predict upcoming rush hours today"""
        predictions = []
        now = datetime.now()
        day_of_week = now.weekday()
        
        # Check next 8 hours
        for hour_offset in range(1, 9):
            target_hour = (now.hour + hour_offset) % 24
            historical_data = self.historical_patterns.get(day_of_week, {}).get(target_hour, [])
            
            if len(historical_data) >= 3:
                avg_customers = np.mean([d["customer_count"] for d in historical_data])
                
                if avg_customers >= 12:
                    target_time = now + timedelta(hours=hour_offset)
                    predictions.append({
                        "time": target_time.strftime("%H:%M"),
                        "predicted_customers": int(avg_customers),
                        "severity": "HIGH" if avg_customers >= 20 else "MEDIUM",
                        "recommended_prep_time": target_time - timedelta(minutes=15)
                    })
        
        return predictions


class DemandForecaster:
    """Forecast product demand and inventory needs"""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.product_velocity = defaultdict(list)  # Sales velocity tracking
    
    def track_product_sale(self, sku: str, timestamp: datetime):
        """Track product sale for velocity calculation"""
        self.product_velocity[sku].append(timestamp)
        
        # Keep only last 24 hours
        cutoff = datetime.now() - timedelta(hours=24)
        self.product_velocity[sku] = [
            t for t in self.product_velocity[sku]
            if t > cutoff
        ]
    
    def predict_stockouts(self, hours_ahead: int = 4) -> List[Dict[str, Any]]:
        """Predict which products will run out of stock"""
        predictions = []
        
        try:
            inventory = self.state_manager.inventory
            
            for sku, current_qty in inventory.items():
                if current_qty <= 0:
                    continue
                
                # Calculate sales velocity (units per hour)
                sales_history = self.product_velocity.get(sku, [])
                
                if len(sales_history) >= 3:
                    hours_of_data = (
                        max(sales_history) - min(sales_history)
                    ).total_seconds() / 3600
                    
                    if hours_of_data > 0:
                        velocity = len(sales_history) / hours_of_data
                        
                        # Predict when stock will run out
                        if velocity > 0:
                            hours_until_stockout = current_qty / velocity
                            
                            if hours_until_stockout <= hours_ahead:
                                product = self.state_manager.products.get(sku, {})
                                predictions.append({
                                    "sku": sku,
                                    "product_name": product.get("product_name", "Unknown"),
                                    "current_quantity": current_qty,
                                    "sales_velocity": round(velocity, 2),
                                    "hours_until_stockout": round(hours_until_stockout, 1),
                                    "urgency": "CRITICAL" if hours_until_stockout <= 1 else "HIGH",
                                    "recommended_restock": int(velocity * 8)  # 8 hours worth
                                })
            
        except Exception as e:
            logger.error(f"Error predicting stockouts: {e}")
        
        return sorted(predictions, key=lambda x: x["hours_until_stockout"])
    
    def get_trending_products(self, hours: int = 4) -> List[Dict[str, Any]]:
        """Identify trending (high-velocity) products"""
        trending = []
        
        cutoff = datetime.now() - timedelta(hours=hours)
        
        for sku, sales_history in self.product_velocity.items():
            recent_sales = [t for t in sales_history if t > cutoff]
            
            if len(recent_sales) >= 5:
                velocity = len(recent_sales) / hours
                product = self.state_manager.products.get(sku, {})
                
                trending.append({
                    "sku": sku,
                    "product_name": product.get("product_name", "Unknown"),
                    "sales_count": len(recent_sales),
                    "velocity": round(velocity, 2),
                    "price": product.get("price", 0)
                })
        
        return sorted(trending, key=lambda x: x["velocity"], reverse=True)[:10]


class PerformancePredictor:
    """Predict system performance and potential issues"""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.performance_history = deque(maxlen=100)
    
    def record_performance_metric(self, metric_name: str, value: float):
        """Record a performance metric"""
        self.performance_history.append({
            "timestamp": datetime.now(),
            "metric": metric_name,
            "value": value
        })
    
    def predict_system_overload(self) -> Optional[Dict[str, Any]]:
        """Predict if system is heading toward overload"""
        try:
            stats = self.state_manager.get_system_stats()
            
            total_customers = stats.get("total_customers_in_queue", 0)
            active_stations = stats.get("active_stations", 1)
            avg_wait = stats.get("average_wait_time", 0)
            
            # Calculate load factor
            max_capacity = active_stations * 6  # 6 customers per station
            load_factor = total_customers / max(max_capacity, 1)
            
            # Calculate trend
            recent_metrics = [
                m for m in self.performance_history
                if m["metric"] == "queue_length"
                and (datetime.now() - m["timestamp"]).total_seconds() <= 300
            ]
            
            if len(recent_metrics) >= 3:
                values = [m["value"] for m in recent_metrics]
                trend = np.polyfit(range(len(values)), values, 1)[0]
                
                if load_factor > 0.8 or (load_factor > 0.6 and trend > 0):
                    minutes_to_overload = self._estimate_time_to_overload(
                        load_factor, trend
                    )
                    
                    return {
                        "event_name": "System Overload Warning",
                        "current_load_factor": round(load_factor, 2),
                        "capacity_used": f"{load_factor*100:.0f}%",
                        "estimated_time_to_overload": f"{minutes_to_overload} minutes",
                        "urgency": "CRITICAL" if minutes_to_overload <= 10 else "HIGH",
                        "recommended_actions": [
                            f"Open {max(1, int((total_customers / 6) - active_stations))} additional stations",
                            "Deploy support staff to assist",
                            "Activate queue management protocols"
                        ]
                    }
            
        except Exception as e:
            logger.error(f"Error predicting system overload: {e}")
        
        return None
    
    def _estimate_time_to_overload(self, current_load: float, trend: float) -> int:
        """Estimate minutes until system overload"""
        if trend <= 0:
            return 999
        
        remaining_capacity = 1.0 - current_load
        minutes = int((remaining_capacity / trend) * 5)
        
        return max(1, min(minutes, 60))
    