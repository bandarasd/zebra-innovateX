#!/usr/bin/env python3
"""
Enhanced Detection Algorithms for Project Sentinel
Implements 7 required algorithms + business intelligence layer
"""

import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, Counter


logger = logging.getLogger(__name__)


class DetectionAlgorithms:
    """Container for all detection algorithms with financial tracking."""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.detection_thresholds = {
            "scan_avoidance_time_window": 10,
            "weight_discrepancy_tolerance": 0.1,
            "long_queue_threshold": 5,
            "extended_wait_threshold": 300,
            "inventory_discrepancy_threshold": 0.15,
            "system_crash_timeout": 60,
            "barcode_switch_confidence_threshold": 0.8,
            "coordinated_fraud_threshold": 3  # Triple mismatch
        }
    
    def _calculate_financial_impact(self, event_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate financial impact for any incident (BACKWARD COMPATIBLE - NEW FIELD)"""
        financial_info = {
            "potential_loss": 0.0,
            "financial_impact": "LOW",
            "revenue_at_risk": 0.0
        }
        
        try:
            if event_type == "Scanner Avoidance":
                sku = data.get("product_sku")
                if sku:
                    product = self.state_manager.products.get(sku, {})
                    loss = product.get("price", 0)
                    financial_info["potential_loss"] = loss
                    financial_info["revenue_at_risk"] = loss
                    
            elif event_type == "Barcode Switching":
                actual_price = data.get("actual_price", 0)
                scanned_price = data.get("scanned_price", 0)
                loss = actual_price - scanned_price
                financial_info["potential_loss"] = loss
                financial_info["revenue_at_risk"] = actual_price
                
            elif event_type == "Weight Discrepancies":
                sku = data.get("product_sku")
                expected = data.get("expected_weight", 0)
                actual = data.get("actual_weight", 0)
                if sku and expected > 0 and actual > expected:
                    product = self.state_manager.products.get(sku, {})
                    price = product.get("price", 0)
                    excess_ratio = (actual - expected) / expected
                    financial_info["potential_loss"] = price * excess_ratio
                    financial_info["revenue_at_risk"] = price
            
            elif event_type == "Inventory Discrepancy":
                sku = data.get("SKU")
                expected = data.get("Expected_Inventory", 0)
                actual = data.get("Actual_Inventory", 0)
                if sku and expected > actual:
                    product = self.state_manager.products.get(sku, {})
                    price = product.get("price", 0)
                    missing_units = expected - actual
                    financial_info["potential_loss"] = price * missing_units
                    financial_info["revenue_at_risk"] = price * expected
            
            # Categorize impact
            loss = financial_info["potential_loss"]
            if loss > 1000:
                financial_info["financial_impact"] = "CRITICAL"
            elif loss > 500:
                financial_info["financial_impact"] = "HIGH"
            elif loss > 100:
                financial_info["financial_impact"] = "MEDIUM"
            else:
                financial_info["financial_impact"] = "LOW"
                
        except Exception as e:
            logger.error(f"Error calculating financial impact: {e}")
        
        return financial_info
    
    # @algorithm Scan Avoidance | Detects when products are taken without proper scanning
    def detect_scan_avoidance(self, rfid_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced scan avoidance with multi-sensor correlation"""
        try:
            station_id = rfid_event.get("station_id")
            epc = rfid_event.get("epc")
            location = rfid_event.get("location")
            sku = rfid_event.get("sku")
            timestamp = rfid_event.get("timestamp")
            
            if not all([station_id, epc, sku, location == "IN_SCAN_AREA"]):
                return None
            
            correlated = self.state_manager.get_correlated_events(timestamp, station_id)
            
            # Check for corresponding POS transaction
            matching_transactions = [
                tx for tx in correlated["transactions"]
                if tx.get("sku") == sku and
                abs((tx.get("timestamp") - timestamp).total_seconds()) <= self.detection_thresholds["scan_avoidance_time_window"]
            ]
            
            if not matching_transactions:
                station = self.state_manager.get_station_state(station_id)
                current_customer = station.current_customer
                
                event_data = {
                    "event_name": "Scanner Avoidance",
                    "station_id": station_id,
                    "customer_id": current_customer or "Unknown",
                    "product_sku": sku,
                    "epc": epc,
                    "confidence": 0.8,
                    "details": {
                        "rfid_timestamp": timestamp.isoformat(),
                        "location": location,
                        "no_matching_transaction": True,
                        "detection_method": "RFID-POS correlation"
                    }
                }
                
                # Add financial impact (NEW - backward compatible)
                financial = self._calculate_financial_impact("Scanner Avoidance", event_data)
                event_data.update(financial)
                
                # Record incident for customer tracking (NEW)
                if current_customer:
                    self.state_manager.record_incident(current_customer, event_data)
                
                return event_data
                
        except Exception as e:
            logger.error(f"Error in scan avoidance detection: {e}")
        
        return None
    
    # @algorithm Barcode Switching | Detects fraudulent barcode replacement
    def detect_barcode_switching(self, transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced barcode switching with vision AI correlation"""
        try:
            station_id = transaction.get("station_id")
            scanned_sku = transaction.get("sku")
            scanned_price = transaction.get("price", 0)
            timestamp = transaction.get("timestamp")
            customer_id = transaction.get("customer_id")
            
            if not all([station_id, scanned_sku, timestamp]):
                return None
            
            correlated = self.state_manager.get_correlated_events(timestamp, station_id)
            
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
                    
                    if predicted_price > scanned_price * 1.2:
                        event_data = {
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
                                "recognition_accuracy": recognition.get("accuracy"),
                                "actual_product_name": predicted_product.get("product_name"),
                                "scanned_product_name": scanned_product.get("product_name")
                            }
                        }
                        
                        # Add financial impact
                        financial = self._calculate_financial_impact("Barcode Switching", event_data)
                        event_data.update(financial)
                        
                        # Record incident
                        if customer_id:
                            self.state_manager.record_incident(customer_id, event_data)
                        
                        return event_data
                        
        except Exception as e:
            logger.error(f"Error in barcode switching detection: {e}")
        
        return None
    
    # @algorithm Weight Discrepancies | Identifies weight mismatches
    def detect_weight_discrepancies(self, transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced weight discrepancy detection"""
        try:
            sku = transaction.get("sku")
            actual_weight = transaction.get("weight_g")
            customer_id = transaction.get("customer_id")
            station_id = transaction.get("station_id")
            
            if not all([sku, actual_weight is not None]):
                return None
            
            product = self.state_manager.products.get(sku, {})
            expected_weight = product.get("weight")
            
            if expected_weight is None:
                return None
            
            weight_diff = abs(actual_weight - expected_weight)
            tolerance = expected_weight * self.detection_thresholds["weight_discrepancy_tolerance"]
            
            if weight_diff > tolerance:
                discrepancy_percentage = (weight_diff / expected_weight) * 100
                
                event_data = {
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
                        "tolerance_threshold": tolerance,
                        "product_name": product.get("product_name"),
                        "exceeds_expected": actual_weight > expected_weight
                    }
                }
                
                # Add financial impact
                financial = self._calculate_financial_impact("Weight Discrepancies", event_data)
                event_data.update(financial)
                
                # Record incident
                if customer_id:
                    self.state_manager.record_incident(customer_id, event_data)
                
                return event_data
                
        except Exception as e:
            logger.error(f"Error in weight discrepancy detection: {e}")
        
        return None
    
    # @algorithm System Crashes | Detects system failures
    def detect_system_crashes(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced system crash detection with downtime tracking"""
        try:
            status = event.get("status")
            station_id = event.get("station_id")
            timestamp = event.get("timestamp")
            event_type = event.get("type")
            
            error_statuses = ["Read Error", "System Crash", "Error", "Offline", "Timeout"]
            
            if status in error_statuses:
                station = self.state_manager.get_station_state(station_id)
                
                # Calculate estimated downtime
                last_active = getattr(station, 'last_active_time', timestamp)
                downtime = (timestamp - last_active).total_seconds() if last_active else 0
                
                event_data = {
                    "event_name": "Unexpected Systems Crash",
                    "station_id": station_id,
                    "error_type": status,
                    "event_source": event_type,
                    "confidence": 1.0,
                    "details": {
                        "error_status": status,
                        "detection_time": timestamp.isoformat(),
                        "affected_system": event_type,
                        "estimated_downtime_seconds": int(downtime),
                        "requires_intervention": status == "System Crash"
                    }
                }
                
                # Track system reliability
                self.state_manager.record_system_error(station_id, event_data)
                
                return event_data
                
        except Exception as e:
            logger.error(f"Error in system crash detection: {e}")
        
        return None
    
    # @algorithm Long Queue Length | Monitors queue buildup
    def detect_long_queue_length(self, queue_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced queue detection with trend analysis"""
        try:
            station_id = queue_event.get("station_id")
            customer_count = queue_event.get("customer_count", 0)
            timestamp = queue_event.get("timestamp")
            
            if customer_count >= self.detection_thresholds["long_queue_threshold"]:
                # Check trend
                station = self.state_manager.get_station_state(station_id)
                trend = self.state_manager.get_queue_trend(station_id)
                
                event_data = {
                    "event_name": "Long Queue Length",
                    "station_id": station_id,
                    "num_of_customers": customer_count,
                    "threshold_exceeded": customer_count - self.detection_thresholds["long_queue_threshold"],
                    "confidence": 0.95,
                    "details": {
                        "detection_time": timestamp.isoformat(),
                        "queue_threshold": self.detection_thresholds["long_queue_threshold"],
                        "trend": trend,  # "increasing", "stable", "decreasing"
                        "severity": "critical" if customer_count >= 10 else "high"
                    }
                }
                
                return event_data
                
        except Exception as e:
            logger.error(f"Error in long queue detection: {e}")
        
        return None
    
    # @algorithm Inventory Discrepancies | Identifies stock inconsistencies
    def detect_inventory_discrepancies(self, inventory_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced inventory discrepancy with trend tracking"""
        try:
            current_inventory = inventory_event.get("data", {})
            timestamp = inventory_event.get("timestamp")
            
            detected_discrepancies = []
            
            for sku, current_qty in current_inventory.items():
                product = self.state_manager.products.get(sku, {})
                expected_qty = product.get("quantity", 0)
                
                if expected_qty > 0:
                    discrepancy_ratio = abs(current_qty - expected_qty) / expected_qty
                    
                    if discrepancy_ratio >= self.detection_thresholds["inventory_discrepancy_threshold"]:
                        detected_discrepancies.append({
                            "sku": sku,
                            "expected": expected_qty,
                            "actual": current_qty,
                            "discrepancy_percentage": round(discrepancy_ratio * 100, 2),
                            "product_name": product.get("product_name", "Unknown"),
                            "unit_price": product.get("price", 0)
                        })
            
            if detected_discrepancies:
                worst_discrepancy = max(detected_discrepancies, 
                                      key=lambda x: x["discrepancy_percentage"])
                
                event_data = {
                    "event_name": "Inventory Discrepancy",
                    "SKU": worst_discrepancy["sku"],
                    "Expected_Inventory": worst_discrepancy["expected"],
                    "Actual_Inventory": worst_discrepancy["actual"],
                    "discrepancy_percentage": worst_discrepancy["discrepancy_percentage"],
                    "confidence": 0.85,
                    "details": {
                        "detection_time": timestamp.isoformat(),
                        "total_discrepancies_found": len(detected_discrepancies),
                        "all_discrepancies": detected_discrepancies,
                        "product_name": worst_discrepancy["product_name"]
                    }
                }
                
                # Add financial impact
                financial = self._calculate_financial_impact("Inventory Discrepancy", event_data)
                event_data.update(financial)
                
                return event_data
                
        except Exception as e:
            logger.error(f"Error in inventory discrepancy detection: {e}")
        
        return None
    
    # @algorithm Extended Wait Times | Tracks excessive dwell times
    def detect_extended_wait_times(self, queue_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced wait time detection with customer impact analysis"""
        try:
            station_id = queue_event.get("station_id")
            avg_dwell_time = queue_event.get("average_dwell_time", 0)
            customer_count = queue_event.get("customer_count", 0)
            timestamp = queue_event.get("timestamp")
            
            if avg_dwell_time >= self.detection_thresholds["extended_wait_threshold"]:
                estimated_wait = avg_dwell_time
                
                if customer_count > 1:
                    estimated_wait = avg_dwell_time * customer_count * 0.5
                
                # Calculate customer satisfaction impact
                satisfaction_score = max(0, 100 - (estimated_wait / 10))
                
                event_data = {
                    "event_name": "Long Wait Time",
                    "station_id": station_id,
                    "wait_time_seconds": round(estimated_wait),
                    "average_dwell_time": avg_dwell_time,
                    "customers_affected": customer_count,
                    "confidence": 0.9,
                    "details": {
                        "detection_time": timestamp.isoformat(),
                        "wait_threshold": self.detection_thresholds["extended_wait_threshold"],
                        "severity": "critical" if estimated_wait > 600 else "high",
                        "estimated_satisfaction_score": round(satisfaction_score, 1),
                        "potential_abandonments": max(0, customer_count - 3) if estimated_wait > 600 else 0
                    }
                }
                
                return event_data
                
        except Exception as e:
            logger.error(f"Error in extended wait time detection: {e}")
        
        return None
    
    # NEW: Coordinated Fraud Detection
    def detect_coordinated_fraud(self, timestamp: datetime, station_id: str) -> Optional[Dict[str, Any]]:
        """Detect complex fraud patterns using multi-sensor correlation"""
        try:
            events = self.state_manager.get_correlated_events(timestamp, station_id)
            
            for transaction in events["transactions"]:
                tx_time = transaction["timestamp"]
                
                nearby_rfid = [
                    r for r in events["rfid_reads"]
                    if abs((r["timestamp"] - tx_time).seconds) <= 5
                ]
                
                nearby_vision = [
                    v for v in events["recognitions"]
                    if abs((v["timestamp"] - tx_time).seconds) <= 5
                ]
                
                if nearby_rfid and nearby_vision:
                    scanned_sku = transaction["sku"]
                    rfid_sku = nearby_rfid[0]["sku"]
                    vision_sku = nearby_vision[0]["predicted_product"]
                    
                    unique_skus = set([scanned_sku, rfid_sku, vision_sku])
                    
                    if len(unique_skus) == 3:  # All three disagree
                        prices = [
                            self.state_manager.products.get(sku, {}).get("price", 0)
                            for sku in unique_skus
                        ]
                        max_loss = max(prices) - min(prices)
                        
                        event_data = {
                            "event_name": "Coordinated Fraud Alert",
                            "station_id": station_id,
                            "customer_id": transaction["customer_id"],
                            "scanned_sku": scanned_sku,
                            "rfid_detected": rfid_sku,
                            "vision_detected": vision_sku,
                            "confidence": 0.95,
                            "severity": "CRITICAL",
                            "potential_loss": max_loss,
                            "financial_impact": "CRITICAL",
                            "details": {
                                "detection_method": "Triple-sensor mismatch",
                                "all_detected_skus": list(unique_skus),
                                "requires_immediate_intervention": True
                            }
                        }
                        
                        # Record high-severity incident
                        customer_id = transaction["customer_id"]
                        self.state_manager.record_incident(customer_id, event_data, severity="CRITICAL")
                        
                        return event_data
                        
        except Exception as e:
            logger.error(f"Error in coordinated fraud detection: {e}")
        
        return None
    
    def generate_operational_insights(self) -> List[Dict[str, Any]]:
        """Enhanced operational insights with predictive recommendations"""
        insights = []
        
        try:
            stats = self.state_manager.get_system_stats()
            total_customers = stats.get("total_customers_in_queue", 0)
            active_stations = stats.get("active_stations", 0)
            
            # Staffing recommendations
            if total_customers > 0:
                customers_per_station = total_customers / max(1, active_stations)
                
                if customers_per_station > 6:
                    additional_stations = max(1, int((total_customers / 6) - active_stations))
                    insights.append({
                        "event_name": "Checkout Station Action",
                        "station_id": "SYSTEM",
                        "Action": "Open",
                        "recommended_additional_stations": additional_stations,
                        "current_ratio": round(customers_per_station, 1),
                        "target_ratio": 6.0,
                        "urgency": "HIGH" if customers_per_station > 10 else "MEDIUM"
                    })
                
                if total_customers >= 10:
                    insights.append({
                        "event_name": "Staffing Needs",
                        "station_id": "SYSTEM",
                        "Staff_type": "Cashier",
                        "urgency": "high" if total_customers >= 15 else "medium",
                        "reason": f"High customer volume: {total_customers} customers",
                        "recommended_count": max(2, total_customers // 8)
                    })
            
            # Individual station insights
            for station_id, station in self.state_manager.stations.items():
                if station.customer_count >= 5:
                    insights.append({
                        "event_name": "Staffing Needs",
                        "station_id": station_id,
                        "Staff_type": "Assistant",
                        "customer_count": station.customer_count,
                        "reason": "Long queue at specific station",
                        "urgency": "MEDIUM"
                    })
            
        except Exception as e:
            logger.error(f"Error generating operational insights: {e}")
        
        return insights


class CorrelationEngine:
    """Enhanced correlation with pattern detection"""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
        self.correlation_window = timedelta(seconds=15)
    
    def correlate_transaction_with_streams(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Multi-stream correlation with confidence scoring"""
        station_id = transaction.get("station_id")
        timestamp = transaction.get("timestamp")
        sku = transaction.get("sku")
        
        if not all([station_id, timestamp, sku]):
            return {"transaction": transaction}
        
        correlated = self.state_manager.get_correlated_events(timestamp, station_id)
        
        matching_rfid = [
            rfid for rfid in correlated["rfid_reads"]
            if rfid.get("sku") == sku and 
            abs((rfid.get("timestamp") - timestamp).total_seconds()) <= 10
        ]
        
        matching_recognition = [
            rec for rec in correlated["recognitions"]
            if abs((rec.get("timestamp") - timestamp).total_seconds()) <= 10
        ]
        
        # Calculate correlation confidence
        correlation_strength = len(matching_rfid) + len(matching_recognition)
        confidence = min(1.0, correlation_strength / 2.0)
        
        return {
            "transaction": transaction,
            "correlated_rfid": matching_rfid,
            "correlated_recognition": matching_recognition,
            "correlation_strength": correlation_strength,
            "correlation_confidence": confidence,
            "all_sensors_agree": self._check_sensor_agreement(
                transaction, matching_rfid, matching_recognition
            )
        }
    
    def _check_sensor_agreement(self, transaction, rfid_list, recognition_list):
        """Check if all sensors agree on the product"""
        if not rfid_list or not recognition_list:
            return None
        
        tx_sku = transaction.get("sku")
        rfid_sku = rfid_list[0].get("sku")
        vision_sku = recognition_list[0].get("predicted_product")
        
        return tx_sku == rfid_sku == vision_sku
    