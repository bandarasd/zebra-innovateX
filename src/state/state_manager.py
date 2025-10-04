#!/usr/bin/env python3
"""
Real-time State Manager for Project Sentinel
Maintains current state of inventory, queues, customers, and transactions.
"""

import logging
import threading
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from collections import defaultdict, deque


logger = logging.getLogger(__name__)


class StationState:
    """State information for a single station (checkout counter)."""
    
    def __init__(self, station_id: str):
        self.station_id = station_id
        self.status = "Active"
        self.customer_count = 0
        self.average_dwell_time = 0.0
        self.current_customer = None
        self.last_update = datetime.now()
        self.recent_transactions = deque(maxlen=10)  # Last 10 transactions
        self.recent_rfid_reads = deque(maxlen=20)    # Last 20 RFID reads
        self.recent_recognitions = deque(maxlen=10)  # Last 10 recognition events
        
    def update_queue_status(self, customer_count: int, avg_dwell_time: float, timestamp: datetime):
        """Update queue monitoring information."""
        self.customer_count = customer_count
        self.average_dwell_time = avg_dwell_time
        self.last_update = timestamp
        
    def add_transaction(self, transaction: Dict[str, Any]):
        """Add a new transaction to the station."""
        self.recent_transactions.append(transaction)
        self.current_customer = transaction.get("customer_id")
        
    def add_rfid_reading(self, rfid_data: Dict[str, Any]):
        """Add a new RFID reading to the station."""
        self.recent_rfid_reads.append(rfid_data)
        
    def add_recognition(self, recognition_data: Dict[str, Any]):
        """Add a new product recognition event to the station."""
        self.recent_recognitions.append(recognition_data)


class CustomerSession:
    """Tracks a customer's session at a station."""
    
    def __init__(self, customer_id: str, station_id: str, start_time: datetime):
        self.customer_id = customer_id
        self.station_id = station_id
        self.start_time = start_time
        self.transactions: List[Dict[str, Any]] = []
        self.rfid_reads: List[Dict[str, Any]] = []
        self.recognitions: List[Dict[str, Any]] = []
        self.end_time: Optional[datetime] = None
        
    def add_transaction(self, transaction: Dict[str, Any]):
        """Add a transaction to this session."""
        self.transactions.append(transaction)
        
    def add_rfid_read(self, rfid_data: Dict[str, Any]):
        """Add an RFID read to this session."""
        self.rfid_reads.append(rfid_data)
        
    def add_recognition(self, recognition_data: Dict[str, Any]):
        """Add a product recognition event to this session."""
        self.recognitions.append(recognition_data)
        
    def get_duration(self) -> float:
        """Get session duration in seconds."""
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()
        
    def end_session(self, end_time: datetime):
        """End the customer session."""
        self.end_time = end_time


class StateManager:
    """Main state manager for the retail analytics system."""
    
    def __init__(self):
        self.lock = threading.RLock()  # Reentrant lock for thread safety
        
        # Current inventory levels
        self.inventory: Dict[str, int] = {}
        self.last_inventory_update: Optional[datetime] = None
        
        # Station states
        self.stations: Dict[str, StationState] = {}
        
        # Active customer sessions
        self.active_sessions: Dict[str, CustomerSession] = {}  # customer_id -> session
        self.session_history: List[CustomerSession] = []
        
        # Reference data
        self.products: Dict[str, Dict[str, Any]] = {}
        self.customers: Dict[str, Dict[str, Any]] = {}
        
        # Event correlation windows
        self.correlation_window = timedelta(seconds=30)  # 30-second correlation window
        
        # System status tracking
        self.system_errors: List[Dict[str, Any]] = []
        self.last_system_check = datetime.now()
        
        logger.info("State manager initialized")
    
    def load_reference_data(self, products: Dict[str, Dict[str, Any]], 
                           customers: Dict[str, Dict[str, Any]]):
        """Load reference data for products and customers."""
        with self.lock:
            self.products = products.copy()
            self.customers = customers.copy()
            logger.info(f"Loaded {len(products)} products and {len(customers)} customers")
    
    def update_inventory(self, inventory_data: Dict[str, Any]):
        """Update current inventory levels."""
        with self.lock:
            timestamp = inventory_data.get("timestamp", datetime.now())
            new_inventory = inventory_data.get("data", {})
            
            # Track inventory changes
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
        """Get or create state for a station."""
        if station_id not in self.stations:
            self.stations[station_id] = StationState(station_id)
        return self.stations[station_id]
    
    def update_queue_monitoring(self, queue_data: Dict[str, Any]):
        """Update queue monitoring information."""
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
    
    def process_pos_transaction(self, transaction: Dict[str, Any]):
        """Process a new POS transaction."""
        with self.lock:
            station_id = transaction.get("station_id")
            customer_id = transaction.get("customer_id")
            
            if not station_id or not customer_id:
                return
            
            # Update station state
            station = self.get_station_state(station_id)
            station.add_transaction(transaction)
            
            # Update or create customer session
            if customer_id not in self.active_sessions:
                self.active_sessions[customer_id] = CustomerSession(
                    customer_id, station_id, transaction.get("timestamp", datetime.now())
                )
            
            session = self.active_sessions[customer_id]
            session.add_transaction(transaction)
            
            # Update inventory if possible
            sku = transaction.get("sku")
            if sku and sku in self.inventory:
                self.inventory[sku] = max(0, self.inventory[sku] - 1)
    
    def process_rfid_reading(self, rfid_data: Dict[str, Any]):
        """Process an RFID reading."""
        with self.lock:
            station_id = rfid_data.get("station_id")
            if not station_id:
                return
                
            station = self.get_station_state(station_id)
            station.add_rfid_reading(rfid_data)
            
            # If there's an active session at this station, add to it
            current_customer = station.current_customer
            if current_customer and current_customer in self.active_sessions:
                self.active_sessions[current_customer].add_rfid_read(rfid_data)
    
    def process_product_recognition(self, recognition_data: Dict[str, Any]):
        """Process a product recognition event."""
        with self.lock:
            station_id = recognition_data.get("station_id")
            if not station_id:
                return
                
            station = self.get_station_state(station_id)
            station.add_recognition(recognition_data)
            
            # If there's an active session at this station, add to it
            current_customer = station.current_customer
            if current_customer and current_customer in self.active_sessions:
                self.active_sessions[current_customer].add_recognition(recognition_data)
    
    def end_customer_session(self, customer_id: str, end_time: Optional[datetime] = None):
        """End a customer session and move to history."""
        with self.lock:
            if customer_id in self.active_sessions:
                session = self.active_sessions.pop(customer_id)
                session.end_session(end_time or datetime.now())
                self.session_history.append(session)
                
                # Clear current customer from station
                for station in self.stations.values():
                    if station.current_customer == customer_id:
                        station.current_customer = None
                
                logger.debug(f"Ended session for customer {customer_id}")
    
    def get_correlated_events(self, reference_time: datetime, 
                             station_id: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Get events within correlation window of reference time."""
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
                # Check transactions
                for transaction in station.recent_transactions:
                    tx_time = transaction.get("timestamp")
                    if tx_time and start_time <= tx_time <= end_time:
                        correlated["transactions"].append(transaction)
                
                # Check RFID reads
                for rfid_read in station.recent_rfid_reads:
                    rfid_time = rfid_read.get("timestamp")
                    if rfid_time and start_time <= rfid_time <= end_time:
                        correlated["rfid_reads"].append(rfid_read)
                
                # Check recognitions
                for recognition in station.recent_recognitions:
                    recog_time = recognition.get("timestamp")
                    if recog_time and start_time <= recog_time <= end_time:
                        correlated["recognitions"].append(recognition)
            
            return correlated
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get current system statistics."""
        with self.lock:
            total_customers = sum(station.customer_count for station in self.stations.values())
            avg_wait_time = sum(station.average_dwell_time for station in self.stations.values()) / max(1, len(self.stations))
            
            active_stations = len([s for s in self.stations.values() if s.customer_count > 0])
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
                "last_inventory_update": self.last_inventory_update.isoformat() if self.last_inventory_update else None
            }
    
    def cleanup_old_sessions(self, max_age_hours: int = 24):
        """Clean up old session history to prevent memory growth."""
        with self.lock:
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            self.session_history = [
                session for session in self.session_history
                if session.end_time and session.end_time > cutoff_time
            ]
    
    def get_station_summary(self, station_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed summary for a specific station."""
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
                "recent_recognitions_count": len(station.recent_recognitions)
            }


# Global state manager instance
state_manager = StateManager()


if __name__ == "__main__":
    # Test the state manager
    logging.basicConfig(level=logging.INFO)
    
    # Test with sample data
    sm = StateManager()
    
    # Test inventory update
    inventory_data = {
        "timestamp": datetime.now(),
        "data": {"PRD_F_01": 100, "PRD_F_02": 80}
    }
    sm.update_inventory(inventory_data)
    
    # Test transaction processing
    transaction = {
        "timestamp": datetime.now(),
        "station_id": "SCC1",
        "customer_id": "C001",
        "sku": "PRD_F_01",
        "price": 280.0,
        "weight_g": 150.0
    }
    sm.process_pos_transaction(transaction)
    
    # Get stats
    stats = sm.get_system_stats()
    print(f"System stats: {stats}")
    
    # Get station summary
    station_summary = sm.get_station_summary("SCC1")
    print(f"Station SCC1: {station_summary}")