#!/usr/bin/env python3
"""
Project Sentinel Main Application
Integrates all components for real-time retail analytics.
"""

import logging
import signal
import sys
import time
import threading
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from streaming.client import StreamingClient
from parsers.data_parsers import StreamDataParser, ReferenceDataLoader
from state.state_manager import StateManager
from algorithms.detection import DetectionAlgorithms
from events.generator import EventGenerator, EventProcessor


logger = logging.getLogger(__name__)


class ProjectSentinel:
    """Main application class for Project Sentinel."""
    
    def __init__(self, config: dict = None):
        self.config = config or self._default_config()
        self.running = False
        
        # Initialize components
        self.state_manager = StateManager()
        self.stream_parser = StreamDataParser()
        self.detection_algorithms = DetectionAlgorithms(self.state_manager)
        self.event_generator = EventGenerator(self.config.get("output_dir", "evidence/output"))
        self.event_processor = EventProcessor(self.detection_algorithms, self.event_generator)
        self.streaming_client = StreamingClient(
            self.config.get("stream_host", "127.0.0.1"),
            self.config.get("stream_port", 8765)
        )
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Background threads
        self.stream_thread = None
        self.stats_thread = None
        
        logger.info("Project Sentinel initialized")
    
    def _default_config(self) -> dict:
        """Default configuration."""
        # Get absolute paths based on the src directory location
        src_dir = Path(__file__).parent
        project_root = src_dir.parent
        
        return {
            "stream_host": "127.0.0.1",
            "stream_port": 8765,
            "output_dir": str(project_root / "evidence" / "output"),
            "data_dir": str(project_root / "data" / "input"),
            "log_level": "INFO",
            "stats_interval": 30  # seconds
        }
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def load_reference_data(self) -> bool:
        """Load product and customer reference data."""
        try:
            data_dir = Path(self.config["data_dir"])
            loader = ReferenceDataLoader()
            
            products = loader.load_products(str(data_dir / "products_list.csv"))
            customers = loader.load_customers(str(data_dir / "customer_data.csv"))
            
            if not products or not customers:
                logger.error("Failed to load reference data")
                return False
            
            self.state_manager.load_reference_data(products, customers)
            logger.info(f"Loaded {len(products)} products and {len(customers)} customers")
            return True
            
        except Exception as e:
            logger.error(f"Error loading reference data: {e}")
            return False
    
    def _setup_stream_handlers(self):
        """Set up handlers for different data streams."""
        
        def handle_inventory(event):
            parsed = self.stream_parser.parse_event(event)
            if parsed:
                self.state_manager.update_inventory(parsed)
                self.event_processor.process_stream_event(parsed)
        
        def handle_pos_transaction(event):
            parsed = self.stream_parser.parse_event(event)
            if parsed:
                self.state_manager.process_pos_transaction(parsed)
                self.event_processor.process_stream_event(parsed)
        
        def handle_rfid_reading(event):
            parsed = self.stream_parser.parse_event(event)
            if parsed:
                self.state_manager.process_rfid_reading(parsed)
                self.event_processor.process_stream_event(parsed)
        
        def handle_queue_monitoring(event):
            parsed = self.stream_parser.parse_event(event)
            if parsed:
                self.state_manager.update_queue_monitoring(parsed)
                self.event_processor.process_stream_event(parsed)
        
        def handle_product_recognition(event):
            parsed = self.stream_parser.parse_event(event)
            if parsed:
                self.state_manager.process_product_recognition(parsed)
                self.event_processor.process_stream_event(parsed)
        
        # Register handlers
        self.streaming_client.register_handler("Current_inventory_data", handle_inventory)
        self.streaming_client.register_handler("POS_Transactions", handle_pos_transaction)
        self.streaming_client.register_handler("RFID_data", handle_rfid_reading)
        self.streaming_client.register_handler("Queue_monitor", handle_queue_monitoring)
        self.streaming_client.register_handler("Product_recognism", handle_product_recognition)
        
        logger.info("Stream handlers configured")
    
    def _stats_monitor(self):
        """Background thread to monitor and log system statistics."""
        while self.running:
            try:
                stats = self.state_manager.get_system_stats()
                event_stats = self.event_generator.get_event_statistics()
                
                logger.info(f"System Stats: {stats['total_customers_in_queue']} customers, "
                           f"{stats['active_stations']} active stations, "
                           f"{event_stats['total_events']} events generated")
                
                # Clean up old sessions periodically
                self.state_manager.cleanup_old_sessions()
                
            except Exception as e:
                logger.error(f"Error in stats monitor: {e}")
            
            time.sleep(self.config.get("stats_interval", 30))
    
    def start(self) -> bool:
        """Start the Project Sentinel system."""
        logger.info("Starting Project Sentinel...")
        
        # Load reference data
        if not self.load_reference_data():
            logger.error("Failed to load reference data, cannot start")
            return False
        
        # Set up stream handlers
        self._setup_stream_handlers()
        
        # Start streaming client in background
        self.running = True
        self.stream_thread = self.streaming_client.start_async()
        
        # Start stats monitoring thread
        self.stats_thread = threading.Thread(target=self._stats_monitor, daemon=True)
        self.stats_thread.start()
        
        logger.info("Project Sentinel started successfully")
        return True
    
    def stop(self):
        """Stop the Project Sentinel system."""
        logger.info("Stopping Project Sentinel...")
        
        self.running = False
        
        # Stop streaming client
        if self.streaming_client:
            self.streaming_client.stop()
        
        # Wait for threads to finish
        if self.stream_thread and self.stream_thread.is_alive():
            self.stream_thread.join(timeout=5)
        
        logger.info("Project Sentinel stopped")
    
    def save_results(self, dataset_type: str = "test") -> bool:
        """Save current results to output files."""
        try:
            success = self.event_generator.save_events(dataset_type)
            if success:
                stats = self.event_generator.get_event_statistics()
                logger.info(f"Saved {stats['total_events']} events to {dataset_type} dataset")
            return success
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            return False
    
    def get_dashboard_data(self) -> dict:
        """Get current data for dashboard display."""
        try:
            system_stats = self.state_manager.get_system_stats()
            event_stats = self.event_generator.get_event_statistics()
            
            # Get recent events (last 10)
            recent_events = self.event_generator.get_events()[-10:] if self.event_generator.get_events() else []
            
            # Get station summaries
            station_summaries = {}
            for station_id in self.state_manager.stations:
                station_summaries[station_id] = self.state_manager.get_station_summary(station_id)
            
            return {
                "system_stats": system_stats,
                "event_stats": event_stats,
                "recent_events": recent_events,
                "station_summaries": station_summaries,
                "inventory_summary": {
                    "total_skus": len(self.state_manager.inventory),
                    "total_items": sum(self.state_manager.inventory.values()) if self.state_manager.inventory else 0,
                    "last_update": system_stats.get("last_inventory_update")
                }
            }
        except Exception as e:
            logger.error(f"Error getting dashboard data: {e}")
            return {}
    
    def run_batch_processing(self, duration_minutes: int = 10) -> bool:
        """
        Run the system for a specified duration and save results.
        Useful for processing test datasets.
        """
        logger.info(f"Starting batch processing for {duration_minutes} minutes...")
        
        if not self.start():
            return False
        
        try:
            # Run for specified duration
            end_time = time.time() + (duration_minutes * 60)
            
            while time.time() < end_time and self.running:
                time.sleep(1)
                
                # Log progress every minute
                remaining = int((end_time - time.time()) / 60)
                if remaining != getattr(self, '_last_remaining', -1):
                    logger.info(f"Batch processing: {remaining} minutes remaining")
                    self._last_remaining = remaining
            
            # Save results
            success = self.save_results("test")
            
            # Log final statistics
            stats = self.event_generator.get_event_statistics()
            logger.info(f"Batch processing completed. Generated {stats['total_events']} events.")
            
            return success
            
        except KeyboardInterrupt:
            logger.info("Batch processing interrupted by user")
            return self.save_results("test")
        
        finally:
            self.stop()


def main():
    """Main entry point."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('project_sentinel.log')
        ]
    )
    
    # Create and run application
    app = ProjectSentinel()
    
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "batch":
            # Batch processing mode
            duration = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            success = app.run_batch_processing(duration)
            sys.exit(0 if success else 1)
        else:
            # Interactive mode
            if app.start():
                logger.info("Project Sentinel is running. Press Ctrl+C to stop.")
                try:
                    while app.running:
                        time.sleep(1)
                except KeyboardInterrupt:
                    logger.info("Shutdown requested by user")
                finally:
                    app.stop()
            else:
                logger.error("Failed to start Project Sentinel")
                sys.exit(1)
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()