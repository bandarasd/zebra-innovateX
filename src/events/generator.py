#!/usr/bin/env python3
"""
Enhanced Event Generator for Project Sentinel
Generates events with business intelligence reports
"""

import json
import logging
import threading
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
from collections import Counter


logger = logging.getLogger(__name__)


class EventGenerator:
    """Enhanced event generator with BI reporting"""
    
    def __init__(self, output_dir: str = "evidence/output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.events: List[Dict[str, Any]] = []
        self.event_counter = 0
        self.lock = threading.Lock()
        
        self.output_files = {
            "test": self.output_dir / "test" / "events.jsonl",
            "final": self.output_dir / "final" / "events.jsonl"
        }
        
        for output_file in self.output_files.values():
            output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # NEW: BI tracking
        self.events_by_type = Counter()
        self.events_by_station = Counter()
        self.total_potential_loss = 0.0
        self.high_severity_events = []
        
        logger.info(f"Enhanced event generator initialized with output dir: {self.output_dir}")
    
    def generate_event_id(self) -> str:
        """Generate a unique event ID"""
        with self.lock:
            event_id = f"E{self.event_counter:03d}"
            self.event_counter += 1
            return event_id
    
    def create_event(self, timestamp: datetime, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create an event in the required format"""
        event = {
            "timestamp": timestamp.isoformat(),
            "event_id": self.generate_event_id(),
            "event_data": event_data.copy()
        }
        
        if not self._validate_event(event):
            logger.warning(f"Generated event failed validation: {event}")
        
        return event
    
    def _validate_event(self, event: Dict[str, Any]) -> bool:
        """Validate event against required schema"""
        required_fields = ["timestamp", "event_id", "event_data"]
        
        for field in required_fields:
            if field not in event:
                logger.error(f"Event missing required field: {field}")
                return False
        
        event_data = event.get("event_data", {})
        if not isinstance(event_data, dict):
            logger.error("event_data must be a dictionary")
            return False
        
        if "event_name" not in event_data:
            logger.error("event_data missing required field: event_name")
            return False
        
        return True
    
    def add_event(self, timestamp: datetime, event_data: Dict[str, Any]):
        """Add a new event to the collection with BI tracking"""
        event = self.create_event(timestamp, event_data)
        
        with self.lock:
            self.events.append(event)
            
            # NEW: Track BI metrics
            event_name = event_data.get("event_name", "Unknown")
            self.events_by_type[event_name] += 1
            
            station_id = event_data.get("station_id", "SYSTEM")
            self.events_by_station[station_id] += 1
            
            potential_loss = event_data.get("potential_loss", 0)
            self.total_potential_loss += potential_loss
            
            # Track high severity events
            if event_data.get("severity") == "CRITICAL" or potential_loss > 500:
                self.high_severity_events.append(event)
            
            logger.debug(f"Added event {event['event_id']}: {event_name}")
    
    def get_events(self) -> List[Dict[str, Any]]:
        """Get all events (thread-safe copy)"""
        with self.lock:
            return self.events.copy()
    
    def clear_events(self):
        """Clear all events and reset BI metrics"""
        with self.lock:
            self.events.clear()
            self.event_counter = 0
            self.events_by_type.clear()
            self.events_by_station.clear()
            self.total_potential_loss = 0.0
            self.high_severity_events.clear()
            logger.info("Cleared all events")
    
    def save_events(self, dataset_type: str = "test") -> bool:
        """Save events to JSONL file"""
        if dataset_type not in self.output_files:
            logger.error(f"Invalid dataset type: {dataset_type}")
            return False
        
        output_file = self.output_files[dataset_type]
        
        try:
            with self.lock:
                events_to_save = self.events.copy()
            
            events_to_save.sort(key=lambda x: x["timestamp"])
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for event in events_to_save:
                    f.write(json.dumps(event) + '\n')
            
            logger.info(f"Saved {len(events_to_save)} events to {output_file}")
            
            # NEW: Save summary report
            self._save_summary_report(dataset_type, events_to_save)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving events to {output_file}: {e}")
            return False
    
    def _save_summary_report(self, dataset_type: str, events: List[Dict[str, Any]]):
        """Save a summary report alongside events"""
        try:
            summary_file = self.output_files[dataset_type].parent / "summary_report.json"
            
            summary = self.get_event_statistics()
            summary["dataset_type"] = dataset_type
            summary["generated_at"] = datetime.now().isoformat()
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"Saved summary report to {summary_file}")
            
        except Exception as e:
            logger.error(f"Error saving summary report: {e}")
    
    def load_events(self, dataset_type: str = "test") -> bool:
        """Load events from JSONL file"""
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
                if loaded_events:
                    max_id = max(int(event["event_id"][1:]) for event in loaded_events if event["event_id"].startswith("E"))
                    self.event_counter = max(self.event_counter, max_id + 1)
            
            logger.info(f"Loaded {len(loaded_events)} events from {input_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading events from {input_file}: {e}")
            return False
    
    def get_event_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about current events"""
        with self.lock:
            events = self.events.copy()
        
        if not events:
            return {"total_events": 0}
        
        event_types = {}
        stations = set()
        customers = set()
        total_loss = 0.0
        high_severity_count = 0
        
        for event in events:
            event_data = event.get("event_data", {})
            event_name = event_data.get("event_name", "Unknown")
            
            event_types[event_name] = event_types.get(event_name, 0) + 1
            
            if "station_id" in event_data:
                stations.add(event_data["station_id"])
            
            if "customer_id" in event_data:
                customers.add(event_data["customer_id"])
            
            # Financial metrics
            total_loss += event_data.get("potential_loss", 0)
            
            if event_data.get("financial_impact") in ["HIGH", "CRITICAL"]:
                high_severity_count += 1
        
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
            "time_range": time_range,
            "financial_summary": {
                "total_potential_loss": round(total_loss, 2),
                "high_severity_events": high_severity_count,
                "average_event_value": round(total_loss / len(events), 2) if events else 0
            },
            "top_event_types": dict(Counter(event_types).most_common(5)),
            "detection_coverage": self._calculate_detection_coverage(event_types)
        }
    
    def _calculate_detection_coverage(self, event_types: Dict[str, int]) -> Dict[str, Any]:
        """Calculate coverage of required detection types"""
        required_detections = {
            "Scanner Avoidance",
            "Barcode Switching",
            "Weight Discrepancies",
            "Unexpected Systems Crash",
            "Long Queue Length",
            "Long Wait Time",
            "Inventory Discrepancy"
        }
        
        detected = set(event_types.keys())
        missing = required_detections - detected
        
        return {
            "required_types": list(required_detections),
            "detected_types": list(detected & required_detections),
            "missing_types": list(missing),
            "coverage_percentage": round(
                len(detected & required_detections) / len(required_detections) * 100,
                1
            )
        }
    
    # NEW: Daily summary report
    def generate_daily_summary(self) -> Dict[str, Any]:
        """Generate end-of-day summary with BI insights"""
        stats = self.get_event_statistics()
        
        with self.lock:
            high_severity = self.high_severity_events.copy()
        
        return {
            "event_name": "Daily Summary Report",
            "generated_at": datetime.now().isoformat(),
            "summary": stats,
            "critical_incidents": [
                {
                    "event_id": e["event_id"],
                    "type": e["event_data"].get("event_name"),
                    "timestamp": e["timestamp"],
                    "loss": e["event_data"].get("potential_loss", 0),
                    "station": e["event_data"].get("station_id"),
                    "customer": e["event_data"].get("customer_id")
                }
                for e in sorted(high_severity, 
                              key=lambda x: x["event_data"].get("potential_loss", 0),
                              reverse=True)[:10]
            ],
            "recommendations": self._generate_recommendations(stats)
        }
    
    def _generate_recommendations(self, stats: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on stats"""
        recommendations = []
        
        event_types = stats.get("event_types", {})
        total_loss = stats.get("financial_summary", {}).get("total_potential_loss", 0)
        
        # Loss-based recommendations
        if total_loss > 1000:
            recommendations.append(
                "URGENT: Potential losses exceed $1000. Implement enhanced security protocols."
            )
        
        # Pattern-based recommendations
        if event_types.get("Scanner Avoidance", 0) > 5:
            recommendations.append(
                "High scan avoidance detected. Consider deploying staff monitors at self-checkout."
            )
        
        if event_types.get("Barcode Switching", 0) > 3:
            recommendations.append(
                "Barcode switching incidents detected. Enhance product recognition system calibration."
            )
        
        if event_types.get("Long Wait Time", 0) > 5:
            recommendations.append(
                "Customer wait times are high. Review staffing allocation and lane management."
            )
        
        # Coverage recommendations
        coverage = stats.get("detection_coverage", {})
        if coverage.get("coverage_percentage", 0) < 100:
            missing = coverage.get("missing_types", [])
            recommendations.append(
                f"Detection coverage at {coverage.get('coverage_percentage')}%. Missing: {', '.join(missing)}"
            )
        
        return recommendations if recommendations else ["No critical issues detected. Continue standard monitoring."]
    
    def create_sample_events(self) -> List[Dict[str, Any]]:
        """Create sample events for testing purposes"""
        now = datetime.now()
        
        sample_events = [
            {
                "event_name": "Scanner Avoidance",
                "station_id": "SCC1",
                "customer_id": "C004",
                "product_sku": "PRD_S_04",
                "potential_loss": 150.0,
                "financial_impact": "MEDIUM"
            },
            {
                "event_name": "Barcode Switching",
                "station_id": "SCC1",
                "customer_id": "C009",
                "actual_sku": "PRD_F_08",
                "scanned_sku": "PRD_F_07",
                "potential_loss": 450.0,
                "financial_impact": "HIGH"
            },
            {
                "event_name": "Weight Discrepancies",
                "station_id": "SCC1",
                "customer_id": "C007",
                "product_sku": "PRD_F_09",
                "expected_weight": 425,
                "actual_weight": 680,
                "potential_loss": 280.0,
                "financial_impact": "MEDIUM"
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
                "Actual_Inventory": 120,
                "potential_loss": 300.0,
                "financial_impact": "MEDIUM"
            }
        ]
        
        for i, event_data in enumerate(sample_events):
            event_time = now + timedelta(minutes=i * 5)
            self.add_event(event_time, event_data)
        
        return self.get_events()


class EventProcessor:
    """Enhanced event processor with BI integration"""
    
    def __init__(self, detection_algorithms, event_generator):
        self.algorithms = detection_algorithms
        self.generator = event_generator
        
    def process_stream_event(self, parsed_event: Dict[str, Any]):
        """Process a parsed stream event and generate alerts if needed"""
        event_type = parsed_event.get("type")
        timestamp = parsed_event.get("timestamp", datetime.now())
        
        detected_events = []
        
        try:
            if event_type == "pos_transaction":
                barcode_event = self.algorithms.detect_barcode_switching(parsed_event)
                if barcode_event:
                    detected_events.append(barcode_event)
                
                weight_event = self.algorithms.detect_weight_discrepancies(parsed_event)
                if weight_event:
                    detected_events.append(weight_event)
                
                # NEW: Check for coordinated fraud
                station_id = parsed_event.get("station_id")
                fraud_event = self.algorithms.detect_coordinated_fraud(timestamp, station_id)
                if fraud_event:
                    detected_events.append(fraud_event)
            
            elif event_type == "rfid_reading":
                avoidance_event = self.algorithms.detect_scan_avoidance(parsed_event)
                if avoidance_event:
                    detected_events.append(avoidance_event)
            
            elif event_type == "queue_monitoring":
                queue_event = self.algorithms.detect_long_queue_length(parsed_event)
                if queue_event:
                    detected_events.append(queue_event)
                
                wait_event = self.algorithms.detect_extended_wait_times(parsed_event)
                if wait_event:
                    detected_events.append(wait_event)
            
            elif event_type == "inventory_snapshot":
                inventory_event = self.algorithms.detect_inventory_discrepancies(parsed_event)
                if inventory_event:
                    detected_events.append(inventory_event)
            
            crash_event = self.algorithms.detect_system_crashes(parsed_event)
            if crash_event:
                detected_events.append(crash_event)
            
            for event_data in detected_events:
                self.generator.add_event(timestamp, event_data)
            
            insights = self.algorithms.generate_operational_insights()
            for insight in insights:
                self.generator.add_event(timestamp, insight)
                
        except Exception as e:
            logger.error(f"Error processing stream event: {e}")
            