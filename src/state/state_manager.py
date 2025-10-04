#!/usr/bin/env python3
"""
Enhanced State Manager for Project Sentinel
Maintains real-time state with customer tracking and incident history
"""

import logging
import threading
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from collections import defaultdict, deque


logger = logging.getLogger(__name__)


class StationState:
    """State information for a single station"""
    
    def __init__(self, station_id: str):
        self.station_id = station_id
        self.status = "Active"
        self.customer_count = 0
        self.average_dwell_time = 0.0
        self.current_customer = None
        self.last_update = datetime.now()
        self.last_active_time = datetime.now()  # NEW: Track uptime
        self.recent_transactions = deque(maxlen=10)
        self.recent_rfid_reads = deque(maxlen=20)
        self.recent_recognitions = deque(maxlen=10)
        self.queue_history = deque(maxlen=20)  # NEW: Track queue changes
        
    def update_queue_status(self, customer_count: int, avg_dwell_time: float, timestamp: datetime):
        """Update queue monitoring information"""
        self.queue_history.append({
            "timestamp": timestamp,
            "customer_count": customer_count,
            "avg_dwell_time": avg_dwell_time
        })
        self.customer_count = customer_count
        self.average_dwell_time = avg_dwell_time
        self.last_update = timestamp
        self.last_active_time = timestamp
        
    def add_transaction(self, transaction: Dict[str, Any]):
        """Add a new transaction to the station"""
        self.recent_transactions.append(transaction)
        self.current_customer = transaction.get("customer_id")
        self.last_active_time = transaction.get("timestamp", datetime.now())
        
    def add_rfid_reading(self, rfid_data: Dict[str, Any]):
        """Add a new RFID reading to the station"""
        self.recent_rfid_reads.append(rfid_data)
        self.last_active_time = rfid_data.get("timestamp", datetime.now())
        
    def add_recognition(self, recognition_data: Dict[str, Any]):
        """Add a new product recognition event to the station"""
        self.recent_recognitions.append(recognition_data)
        self.last_active_time = recognition_data.get("timestamp", datetime.now())


class CustomerSession:
    """Tracks a customer's session at a station"""
    
    def __init__(self, customer_id: str, station_id: str, start_time: datetime):
        self.customer_id = customer_id
        self.station_id = station_id
        self.start_time = start_time
        self.transactions: List[Dict[str, Any]] = []
        self.rfid_reads: List[Dict[str, Any]] = []
        self.recognitions: List[Dict[str, Any]] = []
        self.end_time: Optional[datetime] = None
        
    def add_transaction(self, transaction: Dict[str, Any]):
        """Add a transaction to this session"""
        self.transactions.append(transaction)
        
    def add_rfid_read(self, rfid_data: Dict[str, Any]):
        """Add an RFID read to this session"""
        self.rfid_reads.append(rfid_data)
        
    def add_recognition(self, recognition_data: Dict[str, Any]):
        """Add a product recognition event to this session"""
        self.recognitions.append(recognition_data)
        
    def get_duration(self) -> float:
        """Get session duration in seconds"""
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()
        
    def end_session(self, end_time: datetime):
        """End the customer session"""
        self.end_time = end_time


class StateManager:
    """Enhanced state manager with customer tracking and incident history"""
    
    def __init__(self):
        self.lock = threading.RLock()
        
        # Current inventory levels
        self.inventory: Dict[str, int] = {}
        self.last_inventory_update: Optional[datetime] = None
        
        # Station states
        self.stations: Dict[str, StationState] = {}
        
        # Active customer sessions
        self.active_sessions: Dict[str, CustomerSession] = {}
        self.session_history: List[CustomerSession] = []
        
        # Reference data
        self.products: Dict[str, Dict[str, Any]] = {}
        self.customers: Dict[str, Dict[str, Any]] = {}
        
        # Event correlation
        self.correlation_window = timedelta(seconds=30)
        
        # NEW: Customer incident tracking
        self.customer_incidents: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # NEW: System error tracking
        self.system_errors: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # NEW: Recent incidents for trend analysis
        self.recent_incidents = deque(maxlen=100)
        
        # NEW: Performance metrics
        self.performance_metrics = deque(maxlen=500)
        
        self.last_system_check = datetime.now()
        
        logger.info("Enhanced state manager initialized")
    
    def load_reference_data(self, products: Dict[str, Dict[str, Any]], 
                           customers: Dict[str, Dict[str, Any]]):
        """Load reference data for products and customers"""
        with self.lock:
            self.products = products.copy()
            self.customers = customers.copy()
            logger.info(f"Loaded {len(products)} products and {len(customers)} customers")
    
    def update_inventory(self, inventory_data: Dict[str, Any]):
        """Update current inventory levels"""
        with self.lock:
            timestamp = inventory_data.get("timestamp", datetime.now())
            new_inventory = inventory_data.get("data", {})
            
            if self.inventory:
                changes = {}
                for sku, new_qty in new_inventory.items():
                    old_qty = self.inventory.get(sku, 0)
                    if old_qty != new_qty:
                        changes[sku] = {"old": old_qty, "new": new_qty, "change": new_qty - old_qty}
                
                if changes:
                    logger.debug(f"Inventory changes detected: {len(changes)} SKUs changed")
            
            self.inventory = new_inventory.copy()
            self.last_inventory_update = timestamp
            
    def get_station_state(self, station_id: str) -> StationState:
        """Get or create state for a station"""
        if station_id not in self.stations:
            self.stations[station_id] = StationState(station_id)
        return self.stations[station_id]
    
    def update_queue_monitoring(self, queue_data: Dict[str, Any]):
        """Update queue monitoring information"""
        with self.lock:
            station_id = queue_data.get("station_id")
            if not station_id:
                return
                
            station = self.get_station_state(station_id)
            station.update_queue_status(
                queue_data.get("customer_count", 0),
                queue_data.get("average_dwell_time", 0.0),
                queue_data.get("timestamp", datetime.now())
            )
            
            # NEW: Record performance metric
            self._record_performance_metric(
                "queue_length",
                station_id,
                queue_data.get("customer_count", 0)
            )
    
    def process_pos_transaction(self, transaction: Dict[str, Any]):
        """Process a new POS transaction"""
        with self.lock:
            station_id = transaction.get("station_id")
            customer_id = transaction.get("customer_id")
            
            if not station_id or not customer_id:
                return
            
            station = self.get_station_state(station_id)
            station.add_transaction(transaction)
            
            if customer_id not in self.active_sessions:
                self.active_sessions[customer_id] = CustomerSession(
                    customer_id, station_id, transaction.get("timestamp", datetime.now())
                )
            
            session = self.active_sessions[customer_id]
            session.add_transaction(transaction)
            
            sku = transaction.get("sku")
            if sku and sku in self.inventory:
                self.inventory[sku] = max(0, self.inventory[sku] - 1)
    
    def process_rfid_reading(self, rfid_data: Dict[str, Any]):
        """Process an RFID reading"""
        with self.lock:
            station_id = rfid_data.get("station_id")
            if not station_id:
                return
                
            station = self.get_station_state(station_id)
            station.add_rfid_reading(rfid_data)
            
            current_customer = station.current_customer
            if current_customer and current_customer in self.active_sessions:
                self.active_sessions[current_customer].add_rfid_read(rfid_data)
    
    def process_product_recognition(self, recognition_data: Dict[str, Any]):
        """Process a product recognition event"""
        with self.lock:
            station_id = recognition_data.get("station_id")
            if not station_id:
                return
                
            station = self.get_station_state(station_id)
            station.add_recognition(recognition_data)
            
            current_customer = station.current_customer
            if current_customer and current_customer in self.active_sessions:
                self.active_sessions[current_customer].add_recognition(recognition_data)
    
    def end_customer_session(self, customer_id: str, end_time: Optional[datetime] = None):
        """End a customer session and move to history"""
        with self.lock:
            if customer_id in self.active_sessions:
                session = self.active_sessions.pop(customer_id)
                session.end_session(end_time or datetime.now())
                self.session_history.append(session)
                
                for station in self.stations.values():
                    if station.current_customer == customer_id:
                        station.current_customer = None
                
                logger.debug(f"Ended session for customer {customer_id}")
    
    def get_correlated_events(self, reference_time: datetime, 
                             station_id: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Get events within correlation window of reference time"""
        with self.lock:
            start_time = reference_time - self.correlation_window
            end_time = reference_time + self.correlation_window
            
            correlated = {
                "transactions": [],
                "rfid_reads": [],
                "recognitions": []
            }
            
            stations_to_check = [self.stations[station_id]] if station_id and station_id in self.stations else self.stations.values()
            
            for station in stations_to_check:
                for transaction in station.recent_transactions:
                    tx_time = transaction.get("timestamp")
                    if tx_time and start_time <= tx_time <= end_time:
                        correlated["transactions"].append(transaction)
                
                for rfid_read in station.recent_rfid_reads:
                    rfid_time = rfid_read.get("timestamp")
                    if rfid_time and start_time <= rfid_time <= end_time:
                        correlated["rfid_reads"].append(rfid_read)
                
                for recognition in station.recent_recognitions:
                    recog_time = recognition.get("timestamp")
                    if recog_time and start_time <= recog_time <= end_time:
                        correlated["recognitions"].append(recognition)
            
            return correlated
    
    # NEW: Customer incident tracking
    def record_incident(self, customer_id: str, incident: Dict[str, Any], 
                       severity: str = "MEDIUM"):
        """Record an incident for a customer"""
        with self.lock:
            incident_record = {
                "timestamp": datetime.now(),
                "event_name": incident.get("event_name"),
                "severity": severity,
                "potential_loss": incident.get("potential_loss", 0),
                "station_id": incident.get("station_id"),
                "confidence": incident.get("confidence", 0),
                "details": incident
            }
            
            self.customer_incidents[customer_id].append(incident_record)
            self.recent_incidents.append(incident_record)
            
            # Keep only last 90 days
            cutoff = datetime.now() - timedelta(days=90)
            self.customer_incidents[customer_id] = [
                i for i in self.customer_incidents[customer_id]
                if i["timestamp"] > cutoff
            ]
    
    def get_customer_incidents(self, customer_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get incidents for a customer within specified days"""
        with self.lock:
            cutoff = datetime.now() - timedelta(days=days)
            incidents = self.customer_incidents.get(customer_id, [])
            return [i for i in incidents if i["timestamp"] > cutoff]
    
    def get_recent_incidents(self, minutes: int = 15) -> List[Dict[str, Any]]:
        """Get recent incidents across all customers"""
        with self.lock:
            cutoff = datetime.now() - timedelta(minutes=minutes)
            return [i for i in self.recent_incidents if i["timestamp"] > cutoff]
    
    # NEW: System error tracking
    def record_system_error(self, station_id: str, error: Dict[str, Any]):
        """Record a system error"""
        with self.lock:
            error_record = {
                "timestamp": datetime.now(),
                "station_id": station_id,
                "error_type": error.get("error_type"),
                "details": error
            }
            
            self.system_errors[station_id].append(error_record)
            
            # Keep only last 24 hours
            cutoff = datetime.now() - timedelta(hours=24)
            self.system_errors[station_id] = [
                e for e in self.system_errors[station_id]
                if e["timestamp"] > cutoff
            ]
    
    def get_system_error_rate(self) -> float:
        """Get system-wide error rate"""
        with self.lock:
            total_errors = sum(len(errors) for errors in self.system_errors.values())
            total_events = sum(
                len(station.recent_transactions) + 
                len(station.recent_rfid_reads) + 
                len(station.recent_recognitions)
                for station in self.stations.values()
            )
            
            if total_events == 0:
                return 0.0
            
            return total_errors / total_events
    
    # NEW: Queue trend analysis
    def get_queue_trend(self, station_id: str) -> str:
        """Get queue trend for a station (increasing/stable/decreasing)"""
        with self.lock:
            if station_id not in self.stations:
                return "unknown"
            
            station = self.stations[station_id]
            
            if len(station.queue_history) < 3:
                return "insufficient_data"
            
            recent = list(station.queue_history)[-3:]
            counts = [q["customer_count"] for q in recent]
            
            if counts[-1] > counts[0] * 1.2:
                return "increasing"
            elif counts[-1] < counts[0] * 0.8:
                return "decreasing"
            else:
                return "stable"
    
    # NEW: Performance metric recording
    def _record_performance_metric(self, metric_name: str, station_id: str, value: float):
        """Record a performance metric"""
        self.performance_metrics.append({
            "timestamp": datetime.now(),
            "metric": metric_name,
            "station_id": station_id,
            "value": value
        })
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get current system statistics"""
        with self.lock:
            total_customers = sum(station.customer_count for station in self.stations.values())
            avg_wait_time = sum(station.average_dwell_time for station in self.stations.values()) / max(1, len(self.stations))
            
            active_stations = len([s for s in self.stations.values() if s.customer_count > 0 or s.status == "Active"])
            total_inventory = sum(self.inventory.values()) if self.inventory else 0
            
            return {
                "timestamp": datetime.now().isoformat(),
                "total_customers_in_queue": total_customers,
                "average_wait_time": avg_wait_time,
                "active_stations": active_stations,
                "total_stations": len(self.stations),
                "total_inventory_items": total_inventory,
                "active_sessions": len(self.active_sessions),
                "completed_sessions": len(self.session_history),
                "last_inventory_update": self.last_inventory_update.isoformat() if self.last_inventory_update else None,
                "total_incidents_24h": len(self.recent_incidents),
                "system_error_rate": round(self.get_system_error_rate(), 4)
            }
    
    def cleanup_old_sessions(self, max_age_hours: int = 24):
        """Clean up old session history to prevent memory growth"""
        with self.lock:
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            self.session_history = [
                session for session in self.session_history
                if session.end_time and session.end_time > cutoff_time
            ]
    
    def get_station_summary(self, station_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed summary for a specific station"""
        with self.lock:
            if station_id not in self.stations:
                return None
                
            station = self.stations[station_id]
            return {
                "station_id": station_id,
                "status": station.status,
                "customer_count": station.customer_count,
                "average_dwell_time": station.average_dwell_time,
                "current_customer": station.current_customer,
                "last_update": station.last_update.isoformat(),
                "recent_transactions_count": len(station.recent_transactions),
                "recent_rfid_reads_count": len(station.recent_rfid_reads),
                "recent_recognitions_count": len(station.recent_recognitions),
                "queue_trend": self.get_queue_trend(station_id)
            }


# Global state manager instance
state_manager = StateManager()
