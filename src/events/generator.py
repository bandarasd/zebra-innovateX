#!/usr/bin/env python3
"""
Event Generator for Project Sentinel
Generates events.jsonl output in the required format.
"""

import json
import logging
import threading
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pathlib import Path


logger = logging.getLogger(__name__)


class EventGenerator:
    """Generates and outputs events in the required JSON format."""
    
    def __init__(self, output_dir: str = "evidence/output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Thread-safe event storage
        self.events: List[Dict[str, Any]] = []
        self.event_counter = 0
        self.lock = threading.Lock()
        
        # Output files for different datasets
        self.output_files = {
            "test": self.output_dir / "test" / "events.jsonl",
            "final": self.output_dir / "final" / "events.jsonl"
        }
        
        # Ensure output directories exist
        for output_file in self.output_files.values():
            output_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Event generator initialized with output dir: {self.output_dir}")
    
    def generate_event_id(self) -> str:
        """Generate a unique event ID."""
        with self.lock:
            event_id = f"E{self.event_counter:03d}"
            self.event_counter += 1
            return event_id
    
    def create_event(self, timestamp: datetime, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create an event in the required format.
        
        Required format:
        {
            "timestamp": "2025-08-13T16:05:45",
            "event_id": "E001",
            "event_data": {
                "event_name": "Scanner Avoidance",
                "station_id": "SCC1",
                "customer_id": "C004",
                "product_sku": "PRD_S_04"
            }
        }
        """
        event = {
            "timestamp": timestamp.isoformat(),
            "event_id": self.generate_event_id(),
            "event_data": event_data.copy()
        }
        
        # Validate event format
        if not self._validate_event(event):
            logger.warning(f"Generated event failed validation: {event}")
        
        return event
    
    def _validate_event(self, event: Dict[str, Any]) -> bool:
        """Validate event against required schema."""
        required_fields = ["timestamp", "event_id", "event_data"]
        
        # Check top-level fields
        for field in required_fields:
            if field not in event:
                logger.error(f"Event missing required field: {field}")
                return False
        
        # Check event_data structure
        event_data = event.get("event_data", {})
        if not isinstance(event_data, dict):
            logger.error("event_data must be a dictionary")
            return False
        
        # event_name is required in event_data
        if "event_name" not in event_data:
            logger.error("event_data missing required field: event_name")
            return False
        
        return True
    
    def add_event(self, timestamp: datetime, event_data: Dict[str, Any]):
        """Add a new event to the collection."""
        event = self.create_event(timestamp, event_data)
        
        with self.lock:
            self.events.append(event)
            logger.debug(f"Added event {event['event_id']}: {event_data.get('event_name', 'Unknown')}")
    
    def get_events(self) -> List[Dict[str, Any]]:
        """Get all events (thread-safe copy)."""
        with self.lock:
            return self.events.copy()
    
    def clear_events(self):
        """Clear all events."""
        with self.lock:
            self.events.clear()
            self.event_counter = 0
            logger.info("Cleared all events")
    
    def save_events(self, dataset_type: str = "test") -> bool:
        """
        Save events to JSONL file.
        
        Args:
            dataset_type: Either "test" or "final"
        """
        if dataset_type not in self.output_files:
            logger.error(f"Invalid dataset type: {dataset_type}")
            return False
        
        output_file = self.output_files[dataset_type]
        
        try:
            with self.lock:
                events_to_save = self.events.copy()
            
            # Sort events by timestamp
            events_to_save.sort(key=lambda x: x["timestamp"])
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for event in events_to_save:
                    f.write(json.dumps(event) + '\n')
            
            logger.info(f"Saved {len(events_to_save)} events to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving events to {output_file}: {e}")
            return False
    
    def load_events(self, dataset_type: str = "test") -> bool:
        """Load events from JSONL file."""
        if dataset_type not in self.output_files:
            logger.error(f"Invalid dataset type: {dataset_type}")
            return False
        
        input_file = self.output_files[dataset_type]
        
        if not input_file.exists():
            logger.warning(f"Input file does not exist: {input_file}")
            return False
        
        try:
            loaded_events = []
            with open(input_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        event = json.loads(line)
                        if self._validate_event(event):
                            loaded_events.append(event)
                        else:
                            logger.warning(f"Invalid event at line {line_num}")
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON decode error at line {line_num}: {e}")
            
            with self.lock:
                self.events.extend(loaded_events)
                # Update counter to avoid ID conflicts
                if loaded_events:
                    max_id = max(int(event["event_id"][1:]) for event in loaded_events if event["event_id"].startswith("E"))
                    self.event_counter = max(self.event_counter, max_id + 1)
            
            logger.info(f"Loaded {len(loaded_events)} events from {input_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading events from {input_file}: {e}")
            return False
    
    def get_event_statistics(self) -> Dict[str, Any]:
        """Get statistics about current events."""
        with self.lock:
            events = self.events.copy()
        
        if not events:
            return {"total_events": 0}
        
        # Count events by type
        event_types = {}
        stations = set()
        customers = set()
        
        for event in events:
            event_data = event.get("event_data", {})
            event_name = event_data.get("event_name", "Unknown")
            
            event_types[event_name] = event_types.get(event_name, 0) + 1
            
            if "station_id" in event_data:
                stations.add(event_data["station_id"])
            
            if "customer_id" in event_data:
                customers.add(event_data["customer_id"])
        
        # Time range
        timestamps = [datetime.fromisoformat(event["timestamp"]) for event in events]
        time_range = {
            "start": min(timestamps).isoformat(),
            "end": max(timestamps).isoformat(),
            "duration_seconds": (max(timestamps) - min(timestamps)).total_seconds()
        }
        
        return {
            "total_events": len(events),
            "event_types": event_types,
            "unique_stations": len(stations),
            "unique_customers": len(customers),
            "time_range": time_range
        }
    
    def create_sample_events(self) -> List[Dict[str, Any]]:
        """Create sample events for testing purposes."""
        now = datetime.now()
        
        sample_events = [
            {
                "event_name": "Scanner Avoidance",
                "station_id": "SCC1",
                "customer_id": "C004",
                "product_sku": "PRD_S_04"
            },
            {
                "event_name": "Barcode Switching",
                "station_id": "SCC1",
                "customer_id": "C009",
                "actual_sku": "PRD_F_08",
                "scanned_sku": "PRD_F_07"
            },
            {
                "event_name": "Weight Discrepancies",
                "station_id": "SCC1",
                "customer_id": "C007",
                "product_sku": "PRD_F_09",
                "expected_weight": 425,
                "actual_weight": 680
            },
            {
                "event_name": "Unexpected Systems Crash",
                "station_id": "SCC1",
                "duration_seconds": 180
            },
            {
                "event_name": "Long Queue Length",
                "station_id": "SCC1",
                "num_of_customers": 6
            },
            {
                "event_name": "Long Wait Time",
                "station_id": "SCC1",
                "wait_time_seconds": 350
            },
            {
                "event_name": "Inventory Discrepancy",
                "SKU": "PRD_F_03",
                "Expected_Inventory": 150,
                "Actual_Inventory": 120
            },
            {
                "event_name": "Staffing Needs",
                "station_id": "SCC1",
                "Staff_type": "Cashier"
            },
            {
                "event_name": "Checkout Station Action",
                "station_id": "SCC2",
                "Action": "Open"
            }
        ]
        
        # Add sample events
        for i, event_data in enumerate(sample_events):
            event_time = now + timedelta(minutes=i * 5)
            self.add_event(event_time, event_data)
        
        return self.get_events()


class EventProcessor:
    """Main processor that coordinates detection and event generation."""
    
    def __init__(self, detection_algorithms, event_generator):
        self.algorithms = detection_algorithms
        self.generator = event_generator
        
    def process_stream_event(self, parsed_event: Dict[str, Any]):
        """Process a parsed stream event and generate alerts if needed."""
        event_type = parsed_event.get("type")
        timestamp = parsed_event.get("timestamp", datetime.now())
        
        detected_events = []
        
        try:
            # Run appropriate detection algorithms based on event type
            if event_type == "pos_transaction":
                # Check for barcode switching and weight discrepancies
                barcode_event = self.algorithms.detect_barcode_switching(parsed_event)
                if barcode_event:
                    detected_events.append(barcode_event)
                
                weight_event = self.algorithms.detect_weight_discrepancies(parsed_event)
                if weight_event:
                    detected_events.append(weight_event)
            
            elif event_type == "rfid_reading":
                # Check for scan avoidance
                avoidance_event = self.algorithms.detect_scan_avoidance(parsed_event)
                if avoidance_event:
                    detected_events.append(avoidance_event)
            
            elif event_type == "queue_monitoring":
                # Check for long queues and wait times
                queue_event = self.algorithms.detect_long_queue_length(parsed_event)
                if queue_event:
                    detected_events.append(queue_event)
                
                wait_event = self.algorithms.detect_extended_wait_times(parsed_event)
                if wait_event:
                    detected_events.append(wait_event)
            
            elif event_type == "inventory_snapshot":
                # Check for inventory discrepancies
                inventory_event = self.algorithms.detect_inventory_discrepancies(parsed_event)
                if inventory_event:
                    detected_events.append(inventory_event)
            
            # Check for system crashes (applies to all event types)
            crash_event = self.algorithms.detect_system_crashes(parsed_event)
            if crash_event:
                detected_events.append(crash_event)
            
            # Add detected events to generator
            for event_data in detected_events:
                self.generator.add_event(timestamp, event_data)
            
            # Generate operational insights periodically
            insights = self.algorithms.generate_operational_insights()
            for insight in insights:
                self.generator.add_event(timestamp, insight)
                
        except Exception as e:
            logger.error(f"Error processing stream event: {e}")


if __name__ == "__main__":
    # Test the event generator
    logging.basicConfig(level=logging.INFO)
    
    generator = EventGenerator("test_output")
    
    # Create sample events
    sample_events = generator.create_sample_events()
    print(f"Created {len(sample_events)} sample events")
    
    # Get statistics
    stats = generator.get_event_statistics()
    print(f"Event statistics: {stats}")
    
    # Save to file
    generator.save_events("test")
    
    # Clear and load back
    generator.clear_events()
    generator.load_events("test")
    
    print(f"After reload: {len(generator.get_events())} events")