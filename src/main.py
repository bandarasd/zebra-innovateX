#!/usr/bin/env python3
"""
Enhanced Project Sentinel Main Application
Integrates all components with business intelligence layer
"""

import logging
import signal
import sys
import time
import threading
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from streaming.client import StreamingClient
from parsers.data_parsers import StreamDataParser, ReferenceDataLoader
from state.state_manager import StateManager
from algorithms.detection import DetectionAlgorithms
from algorithms.business_intelligence import (
    BusinessIntelligenceDashboard,
    LossPreventionMetrics,
    CustomerRiskScoring,
    StationHealthMonitor,
    TrendAnalyzer
)
from algorithms.prediction import PredictiveStaffing, DemandForecaster, PerformancePredictor
from events.generator import EventGenerator, EventProcessor


logger = logging.getLogger(__name__)


class ProjectSentinel:
    """Enhanced main application with business intelligence"""
    
    def __init__(self, config: dict = None):
        self.config = config or self._default_config()
        self.running = False
        
        # Core components
        self.state_manager = StateManager()
        self.stream_parser = StreamDataParser()
        self.detection_algorithms = DetectionAlgorithms(self.state_manager)
        self.event_generator = EventGenerator(self.config.get("output_dir", "evidence/output"))
        self.event_processor = EventProcessor(self.detection_algorithms, self.event_generator)
        self.streaming_client = StreamingClient(
            self.config.get("stream_host", "127.0.0.1"),
            self.config.get("stream_port", 8765)
        )
        
        # NEW: Business Intelligence components
        self.bi_dashboard = BusinessIntelligenceDashboard(self.state_manager)
        self.loss_metrics = LossPreventionMetrics(self.state_manager)
        self.risk_scoring = CustomerRiskScoring(self.state_manager)
        self.station_health = StationHealthMonitor(self.state_manager)
        self.trend_analyzer = TrendAnalyzer(self.state_manager)
        
        # NEW: Predictive Analytics
        self.predictive_staffing = PredictiveStaffing(self.state_manager)
        self.demand_forecaster = DemandForecaster(self.state_manager)
        self.performance_predictor = PerformancePredictor(self.state_manager)
        
        # Signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Background threads
        self.stream_thread = None
        self.stats_thread = None
        self.bi_thread = None
        self.prediction_thread = None
        
        logger.info("Enhanced Project Sentinel initialized with Business Intelligence")
    
    def _default_config(self) -> dict:
        """Default configuration"""
        src_dir = Path(__file__).parent
        project_root = src_dir.parent
        
        return {
            "stream_host": "127.0.0.1",
            "stream_port": 8765,
            "output_dir": str(project_root / "evidence" / "output"),
            "data_dir": str(project_root / "data" / "input"),
            "log_level": "INFO",
            "stats_interval": 30,
            "bi_interval": 60,  # NEW: BI reporting interval
            "prediction_interval": 180  # NEW: Prediction interval (3 min)
        }
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def load_reference_data(self) -> bool:
        """Load product and customer reference data"""
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
        """Set up handlers for different data streams"""
        
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
                
                # NEW: Track for demand forecasting
                sku = parsed.get("sku")
                if sku:
                    self.demand_forecaster.track_product_sale(
                        sku, parsed.get("timestamp", datetime.now())
                    )
        
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
                
                # NEW: Record for predictive staffing
                self.predictive_staffing.record_current_state()
        
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
        """Background thread to monitor and log system statistics"""
        while self.running:
            try:
                stats = self.state_manager.get_system_stats()
                event_stats = self.event_generator.get_event_statistics()
                
                logger.info(f"System Stats: {stats['total_customers_in_queue']} customers, "
                           f"{stats['active_stations']} active stations, "
                           f"{event_stats['total_events']} events generated")
                
                self.state_manager.cleanup_old_sessions()
                
            except Exception as e:
                logger.error(f"Error in stats monitor: {e}")
            
            time.sleep(self.config.get("stats_interval", 30))
    
    def _bi_monitor(self):
        """NEW: Background thread for business intelligence reporting"""
        while self.running:
            try:
                # Generate BI insights
                executive_summary = self.bi_dashboard.get_executive_summary()
                
                # Log key priorities
                priorities = executive_summary.get("top_priorities", [])
                if priorities:
                    logger.warning(f"BUSINESS INTELLIGENCE ALERT - Top Priorities:")
                    for i, priority in enumerate(priorities, 1):
                        logger.warning(f"  {i}. {priority}")
                
                # Track financial losses
                kpis = executive_summary.get("kpis", {})
                total_loss = kpis.get("total_potential_loss_today", 0)
                if total_loss > 0:
                    logger.info(f"Loss Prevention: ${total_loss:.2f} in potential losses detected today")
                
                # High-risk customer alerts
                high_risk_count = kpis.get("high_risk_customers", 0)
                if high_risk_count > 0:
                    logger.warning(f"Security Alert: {high_risk_count} high-risk customers identified")
                
                # Station health alerts
                critical_stations = kpis.get("critical_stations", 0)
                if critical_stations > 0:
                    logger.error(f"Maintenance Required: {critical_stations} stations in critical condition")
                
                # Record losses in event generator's BI layer
                for event in self.event_generator.get_events():
                    event_data = event.get("event_data", {})
                    if event_data.get("potential_loss", 0) > 0:
                        self.loss_metrics.record_loss(event_data)
                
            except Exception as e:
                logger.error(f"Error in BI monitor: {e}")
            
            time.sleep(self.config.get("bi_interval", 60))
    
    def _prediction_monitor(self):
        """NEW: Background thread for predictive analytics"""
        while self.running:
            try:
                # Predictive staffing
                staffing_prediction = self.predictive_staffing.predict_next_30_minutes()
                if staffing_prediction:
                    logger.warning(f"PREDICTIVE ALERT: {staffing_prediction['event_name']}")
                    logger.warning(f"  Predicted customers in 30 min: {staffing_prediction['predicted_customers']}")
                    logger.warning(f"  Action needed: {staffing_prediction['action_timeline']}")
                    
                    # Add as event
                    self.event_generator.add_event(
                        datetime.now(),
                        staffing_prediction
                    )
                
                # Stockout predictions
                stockout_predictions = self.demand_forecaster.predict_stockouts(hours_ahead=4)
                if stockout_predictions:
                    logger.warning(f"INVENTORY ALERT: {len(stockout_predictions)} products may run out soon")
                    for prediction in stockout_predictions[:3]:
                        logger.warning(f"  {prediction['product_name']}: {prediction['hours_until_stockout']:.1f} hours remaining")
                
                # System overload prediction
                overload_prediction = self.performance_predictor.predict_system_overload()
                if overload_prediction:
                    logger.error(f"SYSTEM ALERT: {overload_prediction['event_name']}")
                    logger.error(f"  Load: {overload_prediction['capacity_used']}")
                    logger.error(f"  Time to overload: {overload_prediction['estimated_time_to_overload']}")
                    
                    # Add as event
                    self.event_generator.add_event(
                        datetime.now(),
                        overload_prediction
                    )
                
                # Track performance metrics
                stats = self.state_manager.get_system_stats()
                self.performance_predictor.record_performance_metric(
                    "queue_length",
                    stats.get("total_customers_in_queue", 0)
                )
                
            except Exception as e:
                logger.error(f"Error in prediction monitor: {e}")
            
            time.sleep(self.config.get("prediction_interval", 180))
    
    def start(self) -> bool:
        """Start the Project Sentinel system"""
        logger.info("Starting Enhanced Project Sentinel with Business Intelligence...")
        
        if not self.load_reference_data():
            logger.error("Failed to load reference data, cannot start")
            return False
        
        self._setup_stream_handlers()
        
        self.running = True
        self.stream_thread = self.streaming_client.start_async()
        
        self.stats_thread = threading.Thread(target=self._stats_monitor, daemon=True)
        self.stats_thread.start()
        
        # NEW: Start BI monitoring
        self.bi_thread = threading.Thread(target=self._bi_monitor, daemon=True)
        self.bi_thread.start()
        
        # NEW: Start predictive analytics
        self.prediction_thread = threading.Thread(target=self._prediction_monitor, daemon=True)
        self.prediction_thread.start()
        
        logger.info("Project Sentinel started successfully with all BI modules active")
        return True
    
    def stop(self):
        """Stop the Project Sentinel system"""
        logger.info("Stopping Project Sentinel...")
        
        self.running = False
        
        if self.streaming_client:
            self.streaming_client.stop()
        
        if self.stream_thread and self.stream_thread.is_alive():
            self.stream_thread.join(timeout=5)
        
        logger.info("Project Sentinel stopped")
    
    def save_results(self, dataset_type: str = "test") -> bool:
        """Save current results to output files"""
        try:
            success = self.event_generator.save_events(dataset_type)
            if success:
                stats = self.event_generator.get_event_statistics()
                logger.info(f"Saved {stats['total_events']} events to {dataset_type} dataset")
                
                # NEW: Generate and save BI reports
                self._save_bi_reports(dataset_type)
            
            return success
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            return False
    
    def _save_bi_reports(self, dataset_type: str):
        """NEW: Save business intelligence reports"""
        try:
            output_dir = self.event_generator.output_files[dataset_type].parent
            
            # Executive Summary
            executive_summary = self.bi_dashboard.get_executive_summary()
            summary_file = output_dir / "executive_summary.json"
            import json
            with open(summary_file, 'w') as f:
                json.dump(executive_summary, f, indent=2)
            logger.info(f"Saved executive summary to {summary_file}")
            
            # Loss Prevention Report
            loss_summary = self.loss_metrics.get_daily_loss_summary()
            loss_file = output_dir / "loss_prevention_report.json"
            with open(loss_file, 'w') as f:
                json.dump(loss_summary, f, indent=2)
            logger.info(f"Saved loss prevention report to {loss_file}")
            
            # High-Risk Customers
            high_risk = self.risk_scoring.get_high_risk_customers(min_score=30)
            risk_file = output_dir / "high_risk_customers.json"
            with open(risk_file, 'w') as f:
                json.dump(high_risk, f, indent=2)
            logger.info(f"Saved high-risk customers report to {risk_file}")
            
            # Station Health Report
            station_health = self.station_health.get_all_stations_health()
            health_file = output_dir / "station_health_report.json"
            with open(health_file, 'w') as f:
                json.dump(station_health, f, indent=2)
            logger.info(f"Saved station health report to {health_file}")
            
            # Daily Summary
            daily_summary = self.event_generator.generate_daily_summary()
            summary_file = output_dir / "daily_summary.json"
            with open(summary_file, 'w') as f:
                json.dump(daily_summary, f, indent=2)
            logger.info(f"Saved daily summary to {summary_file}")
            
        except Exception as e:
            logger.error(f"Error saving BI reports: {e}")
    
    def get_dashboard_data(self) -> dict:
        """Get current data for dashboard display"""
        try:
            # Core stats
            system_stats = self.state_manager.get_system_stats()
            event_stats = self.event_generator.get_event_statistics()
            
            # NEW: BI data
            executive_summary = self.bi_dashboard.get_executive_summary()
            high_risk_customers = self.risk_scoring.get_high_risk_customers(min_score=60)
            station_health = self.station_health.get_all_stations_health()
            trends = self.trend_analyzer.detect_anomalous_trends()
            
            # NEW: Predictions
            staffing_prediction = self.predictive_staffing.predict_next_30_minutes()
            rush_hours = self.predictive_staffing.predict_rush_hours()
            stockouts = self.demand_forecaster.predict_stockouts(hours_ahead=4)
            trending_products = self.demand_forecaster.get_trending_products()
            
            # Recent events
            recent_events = self.event_generator.get_events()[-10:] if self.event_generator.get_events() else []
            
            # Station summaries
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
                },
                # NEW: Business Intelligence
                "business_intelligence": {
                    "executive_summary": executive_summary,
                    "high_risk_customers": high_risk_customers[:5],
                    "station_health": station_health,
                    "trend_alerts": trends
                },
                # NEW: Predictions
                "predictions": {
                    "staffing": staffing_prediction,
                    "rush_hours": rush_hours,
                    "stockouts": stockouts[:5],
                    "trending_products": trending_products
                }
            }
        except Exception as e:
            logger.error(f"Error getting dashboard data: {e}")
            return {}
    
    def run_batch_processing(self, duration_minutes: int = 10) -> bool:
        """Run the system for a specified duration and save results"""
        logger.info(f"Starting batch processing for {duration_minutes} minutes...")
        
        if not self.start():
            return False
        
        try:
            end_time = time.time() + (duration_minutes * 60)
            
            while time.time() < end_time and self.running:
                time.sleep(1)
                
                remaining = int((end_time - time.time()) / 60)
                if remaining != getattr(self, '_last_remaining', -1):
                    logger.info(f"Batch processing: {remaining} minutes remaining")
                    self._last_remaining = remaining
            
            success = self.save_results("test")
            
            # Log final statistics
            stats = self.event_generator.get_event_statistics()
            logger.info(f"Batch processing completed. Generated {stats['total_events']} events.")
            
            # NEW: Log BI summary
            bi_summary = self.bi_dashboard.get_executive_summary()
            kpis = bi_summary.get("kpis", {})
            logger.info(f"Business Intelligence Summary:")
            logger.info(f"  Total Potential Loss: ${kpis.get('total_potential_loss_today', 0):.2f}")
            logger.info(f"  High-Risk Customers: {kpis.get('high_risk_customers', 0)}")
            logger.info(f"  Critical Stations: {kpis.get('critical_stations', 0)}")
            
            return success
            
        except KeyboardInterrupt:
            logger.info("Batch processing interrupted by user")
            return self.save_results("test")
        
        finally:
            self.stop()


def main():
    """Main entry point"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('project_sentinel.log')
        ]
    )
    
    app = ProjectSentinel()
    
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "batch":
            duration = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            success = app.run_batch_processing(duration)
            sys.exit(0 if success else 1)
        else:
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
    