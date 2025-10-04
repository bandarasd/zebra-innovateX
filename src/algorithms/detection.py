#!/usr/bin/env python3
"""
Detection Algorithms for Project Sentinel
Implements the 7 required anomaly detection algorithms.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict


logger = logging.getLogger(__name__)


class DetectionAlgorithms:
    """Container for all detection algorithms with proper tagging."""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.detection_thresholds = {
            "scan_avoidance_time_window": 10,  # seconds
            "weight_discrepancy_tolerance": 0.1,  # 10% tolerance
            "long_queue_threshold": 5,  # customers
            "extended_wait_threshold": 300,  # seconds (5 minutes)
            "inventory_discrepancy_threshold": 0.15,  # 15% discrepancy
            "system_crash_timeout": 60,  # seconds
            "barcode_switch_confidence_threshold": 0.8
        }
    
    # @algorithm Scan Avoidance | Detects when products are taken without proper scanning
    def detect_scan_avoidance(self, rfid_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect scan avoidance by correlating RFID reads with POS transactions.
        When RFID detects a product in scan area but no corresponding transaction occurs.
        """
        try:
            station_id = rfid_event.get("station_id")
            epc = rfid_event.get("epc")
            location = rfid_event.get("location")
            sku = rfid_event.get("sku")
            timestamp = rfid_event.get("timestamp")
            
            # Only process when product enters scan area
            if not all([station_id, epc, sku, location == "IN_SCAN_AREA"]):
                return None
            
            # Get correlated events within time window
            correlated = self.state_manager.get_correlated_events(timestamp, station_id)
            
            # Check for corresponding POS transaction
            matching_transactions = [
                tx for tx in correlated["transactions"]
                if tx.get("sku") == sku and
                abs((tx.get("timestamp") - timestamp).total_seconds()) <= self.detection_thresholds["scan_avoidance_time_window"]
            ]
            
            # If RFID detected product but no transaction, it's potential scan avoidance
            if not matching_transactions:
                station = self.state_manager.get_station_state(station_id)
                current_customer = station.current_customer
                
                return {
                    "event_name": "Scanner Avoidance",
                    "station_id": station_id,
                    "customer_id": current_customer or "Unknown",
                    "product_sku": sku,
                    "epc": epc,
                    "confidence": 0.8,
                    "details": {
                        "rfid_timestamp": timestamp.isoformat(),
                        "location": location,
                        "no_matching_transaction": True
                    }
                }
                
        except Exception as e:
            logger.error(f"Error in scan avoidance detection: {e}")
        
        return None
    
    # @algorithm Barcode Switching | Detects fraudulent barcode replacement with cheaper alternatives
    def detect_barcode_switching(self, transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect barcode switching by comparing scanned product with product recognition predictions.
        """
        try:
            station_id = transaction.get("station_id")
            scanned_sku = transaction.get("sku")
            scanned_price = transaction.get("price", 0)
            timestamp = transaction.get("timestamp")
            customer_id = transaction.get("customer_id")
            
            if not all([station_id, scanned_sku, timestamp]):
                return None
            
            # Get correlated product recognition events
            correlated = self.state_manager.get_correlated_events(timestamp, station_id)
            
            # Find matching recognition event
            matching_recognitions = [
                rec for rec in correlated["recognitions"]
                if abs((rec.get("timestamp") - timestamp).total_seconds()) <= 10 and
                rec.get("accuracy", 0) >= self.detection_thresholds["barcode_switch_confidence_threshold"]
            ]
            
            if matching_recognitions:
                recognition = matching_recognitions[0]
                predicted_sku = recognition.get("predicted_product")
                predicted_product = self.state_manager.products.get(predicted_sku, {})
                scanned_product = self.state_manager.products.get(scanned_sku, {})
                
                if predicted_sku != scanned_sku and predicted_product and scanned_product:
                    predicted_price = predicted_product.get("price", 0)
                    
                    # Check if scanned item is significantly cheaper
                    if predicted_price > scanned_price * 1.2:  # 20% price difference threshold
                        return {
                            "event_name": "Barcode Switching",
                            "station_id": station_id,
                            "customer_id": customer_id,
                            "actual_sku": predicted_sku,
                            "scanned_sku": scanned_sku,
                            "actual_price": predicted_price,
                            "scanned_price": scanned_price,
                            "confidence": recognition.get("accuracy"),
                            "details": {
                                "price_difference": predicted_price - scanned_price,
                                "recognition_accuracy": recognition.get("accuracy")
                            }
                        }
                        
        except Exception as e:
            logger.error(f"Error in barcode switching detection: {e}")
        
        return None
    
    # @algorithm Weight Discrepancies | Identifies mismatches between expected and actual product weights
    def detect_weight_discrepancies(self, transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect weight discrepancies by comparing transaction weight with expected product weight.
        """
        try:
            sku = transaction.get("sku")
            actual_weight = transaction.get("weight_g")
            customer_id = transaction.get("customer_id")
            station_id = transaction.get("station_id")
            
            if not all([sku, actual_weight is not None]):
                return None
            
            # Get expected weight from product catalog
            product = self.state_manager.products.get(sku, {})
            expected_weight = product.get("weight")
            
            if expected_weight is None:
                return None
            
            # Calculate weight discrepancy
            weight_diff = abs(actual_weight - expected_weight)
            tolerance = expected_weight * self.detection_thresholds["weight_discrepancy_tolerance"]
            
            if weight_diff > tolerance:
                discrepancy_percentage = (weight_diff / expected_weight) * 100
                
                return {
                    "event_name": "Weight Discrepancies",
                    "station_id": station_id,
                    "customer_id": customer_id,
                    "product_sku": sku,
                    "expected_weight": expected_weight,
                    "actual_weight": actual_weight,
                    "discrepancy_percentage": round(discrepancy_percentage, 2),
                    "confidence": 0.9,
                    "details": {
                        "weight_difference": weight_diff,
                        "tolerance_threshold": tolerance
                    }
                }
                
        except Exception as e:
            logger.error(f"Error in weight discrepancy detection: {e}")
        
        return None
    
    # @algorithm System Crashes | Detects unexpected system failures and scanning errors
    def detect_system_crashes(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect system crashes and scanning errors from status fields and error patterns.
        """
        try:
            status = event.get("status")
            station_id = event.get("station_id")
            timestamp = event.get("timestamp")
            event_type = event.get("type")
            
            # Check for error status indicators
            error_statuses = ["Read Error", "System Crash", "Error", "Offline", "Timeout"]
            
            if status in error_statuses:
                # Calculate downtime/error duration if possible
                station = self.state_manager.get_station_state(station_id)
                
                return {
                    "event_name": "Unexpected Systems Crash",
                    "station_id": station_id,
                    "error_type": status,
                    "event_source": event_type,
                    "confidence": 1.0,
                    "details": {
                        "error_status": status,
                        "detection_time": timestamp.isoformat(),
                        "affected_system": event_type
                    }
                }
                
        except Exception as e:
            logger.error(f"Error in system crash detection: {e}")
        
        return None
    
    # @algorithm Long Queue Length | Monitors and alerts on excessive customer queue buildup
    def detect_long_queue_length(self, queue_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect when queue length exceeds acceptable thresholds.
        """
        try:
            station_id = queue_event.get("station_id")
            customer_count = queue_event.get("customer_count", 0)
            timestamp = queue_event.get("timestamp")
            
            if customer_count >= self.detection_thresholds["long_queue_threshold"]:
                return {
                    "event_name": "Long Queue Length",
                    "station_id": station_id,
                    "num_of_customers": customer_count,
                    "threshold_exceeded": customer_count - self.detection_thresholds["long_queue_threshold"],
                    "confidence": 0.95,
                    "details": {
                        "detection_time": timestamp.isoformat(),
                        "queue_threshold": self.detection_thresholds["long_queue_threshold"]
                    }
                }
                
        except Exception as e:
            logger.error(f"Error in long queue detection: {e}")
        
        return None
    
    # @algorithm Inventory Discrepancies | Identifies stock level inconsistencies between systems
    def detect_inventory_discrepancies(self, inventory_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect inventory discrepancies by comparing current levels with expected levels.
        """
        try:
            current_inventory = inventory_event.get("data", {})
            timestamp = inventory_event.get("timestamp")
            
            detected_discrepancies = []
            
            for sku, current_qty in current_inventory.items():
                product = self.state_manager.products.get(sku, {})
                expected_qty = product.get("quantity", 0)
                
                if expected_qty > 0:  # Only check items that should have stock
                    discrepancy_ratio = abs(current_qty - expected_qty) / expected_qty
                    
                    if discrepancy_ratio >= self.detection_thresholds["inventory_discrepancy_threshold"]:
                        detected_discrepancies.append({
                            "sku": sku,
                            "expected": expected_qty,
                            "actual": current_qty,
                            "discrepancy_percentage": round(discrepancy_ratio * 100, 2)
                        })
            
            if detected_discrepancies:
                # Return the most significant discrepancy
                worst_discrepancy = max(detected_discrepancies, 
                                      key=lambda x: x["discrepancy_percentage"])
                
                return {
                    "event_name": "Inventory Discrepancy",
                    "SKU": worst_discrepancy["sku"],
                    "Expected_Inventory": worst_discrepancy["expected"],
                    "Actual_Inventory": worst_discrepancy["actual"],
                    "discrepancy_percentage": worst_discrepancy["discrepancy_percentage"],
                    "confidence": 0.85,
                    "details": {
                        "detection_time": timestamp.isoformat(),
                        "total_discrepancies_found": len(detected_discrepancies),
                        "all_discrepancies": detected_discrepancies
                    }
                }
                
        except Exception as e:
            logger.error(f"Error in inventory discrepancy detection: {e}")
        
        return None
    
    # @algorithm Extended Wait Times | Tracks and alerts on excessive customer dwell times
    def detect_extended_wait_times(self, queue_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect when customer wait times exceed acceptable thresholds.
        """
        try:
            station_id = queue_event.get("station_id")
            avg_dwell_time = queue_event.get("average_dwell_time", 0)
            customer_count = queue_event.get("customer_count", 0)
            timestamp = queue_event.get("timestamp")
            
            if avg_dwell_time >= self.detection_thresholds["extended_wait_threshold"]:
                # Estimate individual customer wait time
                estimated_wait = avg_dwell_time
                
                # If multiple customers, the wait might be even longer for later customers
                if customer_count > 1:
                    estimated_wait = avg_dwell_time * customer_count * 0.5  # Rough estimation
                
                return {
                    "event_name": "Long Wait Time",
                    "station_id": station_id,
                    "wait_time_seconds": round(estimated_wait),
                    "average_dwell_time": avg_dwell_time,
                    "customers_affected": customer_count,
                    "confidence": 0.9,
                    "details": {
                        "detection_time": timestamp.isoformat(),
                        "wait_threshold": self.detection_thresholds["extended_wait_threshold"],
                        "severity": "high" if estimated_wait > 600 else "medium"
                    }
                }
                
        except Exception as e:
            logger.error(f"Error in extended wait time detection: {e}")
        
        return None
    
    def generate_operational_insights(self) -> List[Dict[str, Any]]:
        """
        Generate actionable operational insights based on current state.
        """
        insights = []
        
        try:
            stats = self.state_manager.get_system_stats()
            total_customers = stats.get("total_customers_in_queue", 0)
            active_stations = stats.get("active_stations", 0)
            total_stations = stats.get("total_stations", 1)
            avg_wait_time = stats.get("average_wait_time", 0)
            
            # Staffing recommendations
            if total_customers > 0:
                customers_per_station = total_customers / max(1, active_stations)
                
                if customers_per_station > 6:  # Target ratio from requirements
                    additional_stations = max(1, int((total_customers / 6) - active_stations))
                    insights.append({
                        "event_name": "Checkout Station Action",
                        "station_id": "SYSTEM",
                        "Action": "Open",
                        "recommended_additional_stations": additional_stations,
                        "current_ratio": round(customers_per_station, 1),
                        "target_ratio": 6.0
                    })
                
                # Staffing needs based on queue length
                if total_customers >= 10:
                    insights.append({
                        "event_name": "Staffing Needs",
                        "station_id": "SYSTEM",
                        "Staff_type": "Cashier",
                        "urgency": "high" if total_customers >= 15 else "medium",
                        "reason": f"High customer volume: {total_customers} customers"
                    })
            
            # Check individual stations for specific issues
            for station_id, station in self.state_manager.stations.items():
                if station.customer_count >= 5:
                    insights.append({
                        "event_name": "Staffing Needs",
                        "station_id": station_id,
                        "Staff_type": "Assistant",
                        "customer_count": station.customer_count,
                        "reason": "Long queue at specific station"
                    })
            
        except Exception as e:
            logger.error(f"Error generating operational insights: {e}")
        
        return insights


# Correlation engine for matching events across streams
class CorrelationEngine:
    """Correlates events across different data streams for comprehensive analysis."""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.correlation_window = timedelta(seconds=15)
    
    def correlate_transaction_with_streams(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Correlate a POS transaction with RFID reads and product recognition events.
        """
        station_id = transaction.get("station_id")
        timestamp = transaction.get("timestamp")
        sku = transaction.get("sku")
        
        if not all([station_id, timestamp, sku]):
            return {"transaction": transaction}
        
        # Get correlated events
        correlated = self.state_manager.get_correlated_events(timestamp, station_id)
        
        # Find matching RFID reads
        matching_rfid = [
            rfid for rfid in correlated["rfid_reads"]
            if rfid.get("sku") == sku and 
            abs((rfid.get("timestamp") - timestamp).total_seconds()) <= 10
        ]
        
        # Find matching product recognition
        matching_recognition = [
            rec for rec in correlated["recognitions"]
            if abs((rec.get("timestamp") - timestamp).total_seconds()) <= 10
        ]
        
        return {
            "transaction": transaction,
            "correlated_rfid": matching_rfid,
            "correlated_recognition": matching_recognition,
            "correlation_strength": len(matching_rfid) + len(matching_recognition)
        }


if __name__ == "__main__":
    # Test the detection algorithms
    logging.basicConfig(level=logging.INFO)
    
    from state.state_manager import StateManager
    
    # Create test instances
    sm = StateManager()
    algorithms = DetectionAlgorithms(sm)
    
    # Test with sample data
    test_transaction = {
        "timestamp": datetime.now(),
        "station_id": "SCC1",
        "customer_id": "C001",
        "sku": "PRD_F_01",
        "weight_g": 200.0  # Expected: 150g
    }
    
    # Load sample product data
    sm.products = {
        "PRD_F_01": {"weight": 150.0, "price": 280.0}
    }
    
    # Test weight discrepancy detection
    result = algorithms.detect_weight_discrepancies(test_transaction)
    if result:
        print(f"Weight discrepancy detected: {result}")
    else:
        print("No weight discrepancy detected")