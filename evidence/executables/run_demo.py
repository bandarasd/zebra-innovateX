#!/usr/bin/env python3
"""
Automation script for Project Sentinel judges.
This script sets up dependencies, starts services, and generates all required outputs.
"""

import os
import sys
import subprocess
import time
import shutil
import logging
import signal
from pathlib import Path
from datetime import datetime


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('run_demo.log')
    ]
)
logger = logging.getLogger(__name__)


class DemoRunner:
    """Automated demo runner for judges."""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent.parent  # Go up to project root
        self.src_dir = self.base_dir / "src"
        self.data_dir = self.base_dir / "data"
        self.results_dir = Path("./results")  # Relative to executables directory
        self.evidence_dir = self.base_dir / "evidence"
        
        # Process handles
        self.streaming_server_process = None
        self.main_app_process = None
        
        logger.info(f"Demo runner initialized with base dir: {self.base_dir}")
    
    def check_python_version(self):
        """Check Python version compatibility."""
        version = sys.version_info
        if version.major < 3 or (version.major == 3 and version.minor < 9):
            raise RuntimeError(f"Python 3.9+ required, found {version.major}.{version.minor}")
        logger.info(f"Python version check passed: {version.major}.{version.minor}.{version.micro}")
    
    def setup_environment(self):
        """Set up the environment and verify dependencies."""
        logger.info("Setting up environment...")
        
        # Check Python version
        self.check_python_version()
        
        # Add src directory to Python path
        src_path = str(self.src_dir.absolute())
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
        
        # Create results directory
        self.results_dir.mkdir(exist_ok=True)
        
        # Verify required directories exist
        required_dirs = [self.src_dir, self.data_dir]
        for dir_path in required_dirs:
            if not dir_path.exists():
                raise FileNotFoundError(f"Required directory not found: {dir_path}")
        
        # Check for required data files
        required_files = [
            self.data_dir / "input" / "products_list.csv",
            self.data_dir / "input" / "customer_data.csv",
            self.data_dir / "streaming-server" / "stream_server.py"
        ]
        
        for file_path in required_files:
            if not file_path.exists():
                raise FileNotFoundError(f"Required file not found: {file_path}")
        
        logger.info("Environment setup completed")
    
    def start_streaming_server(self, port=8765, speed=10):
        """Start the streaming server."""
        logger.info(f"Starting streaming server on port {port} with speed {speed}x...")
        
        server_script = self.data_dir / "streaming-server" / "stream_server.py"
        cmd = [
            sys.executable, str(server_script),
            "--port", str(port),
            "--speed", str(speed),
            "--loop",
            "--host", "127.0.0.1"
        ]
        
        try:
            self.streaming_server_process = subprocess.Popen(
                cmd,
                cwd=str(self.data_dir / "streaming-server"),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Give server time to start
            time.sleep(3)
            
            # Check if server started successfully
            if self.streaming_server_process.poll() is not None:
                stdout, stderr = self.streaming_server_process.communicate()
                raise RuntimeError(f"Streaming server failed to start: {stderr}")
            
            logger.info("Streaming server started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start streaming server: {e}")
            raise
    
    def run_analytics_system(self, duration_minutes=5):
        """Run the main analytics system."""
        logger.info(f"Starting analytics system for {duration_minutes} minutes...")
        
        main_script = self.src_dir / "main.py"
        cmd = [sys.executable, str(main_script), "batch", str(duration_minutes)]
        
        try:
            self.main_app_process = subprocess.Popen(
                cmd,
                cwd=str(self.src_dir),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            
            # Monitor the process
            logger.info("Analytics system running...")
            
            # Stream output in real-time
            while True:
                output = self.main_app_process.stdout.readline()
                if output == '' and self.main_app_process.poll() is not None:
                    break
                if output:
                    logger.info(f"APP: {output.strip()}")
            
            # Get return code
            return_code = self.main_app_process.poll()
            
            if return_code == 0:
                logger.info("Analytics system completed successfully")
                return True
            else:
                logger.error(f"Analytics system failed with return code: {return_code}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to run analytics system: {e}")
            return False
    
    def copy_results(self):
        """Copy results to the results directory."""
        logger.info("Copying results...")
        
        try:
            # Create output structure in results
            results_output = self.results_dir / "output"
            results_output.mkdir(exist_ok=True)
            
            # Copy events.jsonl files
            evidence_output = self.evidence_dir / "output"
            
            if evidence_output.exists():
                # Copy test results
                test_dir = evidence_output / "test"
                if test_dir.exists():
                    results_test = results_output / "test"
                    results_test.mkdir(exist_ok=True)
                    
                    test_events = test_dir / "events.jsonl"
                    if test_events.exists():
                        shutil.copy2(test_events, results_test / "events.jsonl")
                        logger.info("Copied test events.jsonl")
                
                # Copy final results
                final_dir = evidence_output / "final"
                if final_dir.exists():
                    results_final = results_output / "final"
                    results_final.mkdir(exist_ok=True)
                    
                    final_events = final_dir / "events.jsonl"
                    if final_events.exists():
                        shutil.copy2(final_events, results_final / "events.jsonl")
                        logger.info("Copied final events.jsonl")
            
            # Create a summary report
            self.generate_summary_report()
            
            logger.info("Results copied successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy results: {e}")
            return False
    
    def generate_summary_report(self):
        """Generate a summary report of the run."""
        try:
            # Count events in each output file
            test_events = 0
            final_events = 0
            
            test_file = self.results_dir / "output" / "test" / "events.jsonl"
            if test_file.exists():
                with open(test_file, 'r') as f:
                    test_events = sum(1 for line in f if line.strip())
            
            final_file = self.results_dir / "output" / "final" / "events.jsonl"
            if final_file.exists():
                with open(final_file, 'r') as f:
                    final_events = sum(1 for line in f if line.strip())
            
            # Create summary
            summary = {
                "run_timestamp": datetime.now().isoformat(),
                "test_events_generated": test_events,
                "final_events_generated": final_events,
                "total_events": test_events + final_events,
                "demo_completed": True
            }
            
            # Write summary
            import json
            with open(self.results_dir / "summary.json", 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"Generated summary: {test_events} test events, {final_events} final events")
            
        except Exception as e:
            logger.error(f"Failed to generate summary: {e}")
    
    def cleanup(self):
        """Clean up processes and resources."""
        logger.info("Cleaning up...")
        
        # Stop main application
        if self.main_app_process and self.main_app_process.poll() is None:
            try:
                self.main_app_process.terminate()
                self.main_app_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.main_app_process.kill()
            except Exception as e:
                logger.warning(f"Error stopping main app: {e}")
        
        # Stop streaming server
        if self.streaming_server_process and self.streaming_server_process.poll() is None:
            try:
                self.streaming_server_process.terminate()
                self.streaming_server_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.streaming_server_process.kill()
            except Exception as e:
                logger.warning(f"Error stopping streaming server: {e}")
        
        logger.info("Cleanup completed")
    
    def run_demo(self, duration_minutes=5):
        """Run the complete demo process."""
        logger.info("=" * 60)
        logger.info("PROJECT SENTINEL DEMO STARTING")
        logger.info("=" * 60)
        
        try:
            # Setup
            self.setup_environment()
            
            # Start streaming server
            self.start_streaming_server()
            
            # Run analytics system
            success = self.run_analytics_system(duration_minutes)
            
            if success:
                # Copy results
                self.copy_results()
                logger.info("Demo completed successfully!")
                return True
            else:
                logger.error("Demo failed during analytics processing")
                return False
                
        except Exception as e:
            logger.error(f"Demo failed: {e}")
            return False
        
        finally:
            self.cleanup()


def main():
    """Main entry point for the demo runner."""
    
    # Parse command line arguments
    duration = 5  # Default 5 minutes
    if len(sys.argv) > 1:
        try:
            duration = int(sys.argv[1])
        except ValueError:
            logger.warning(f"Invalid duration '{sys.argv[1]}', using default 5 minutes")
    
    # Set up signal handlers for graceful shutdown
    demo_runner = DemoRunner()
    
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, cleaning up...")
        demo_runner.cleanup()
        sys.exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the demo
    try:
        success = demo_runner.run_demo(duration)
        
        if success:
            logger.info("=" * 60)
            logger.info("DEMO COMPLETED SUCCESSFULLY")
            logger.info("Check ./results/ directory for output files")
            logger.info("=" * 60)
            sys.exit(0)
        else:
            logger.error("=" * 60)
            logger.error("DEMO FAILED")
            logger.error("Check logs for details")
            logger.error("=" * 60)
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
        demo_runner.cleanup()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        demo_runner.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    main()