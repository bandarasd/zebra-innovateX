#!/usr/bin/env python3
"""
Project Sentinel Streaming Client
Connects to the streaming server and processes real-time data streams.
"""

import json
import socket
import logging
import threading
import time
from typing import Iterator, Dict, Any, Callable, Optional
from datetime import datetime


logger = logging.getLogger(__name__)


class StreamingClient:
    """Robust streaming client for Project Sentinel data streams."""
    
    def __init__(self, host: str = "127.0.0.1", port: int = 8765):
        self.host = host
        self.port = port
        self.running = False
        self.connection = None
        self.event_handlers: Dict[str, Callable] = {}
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 2  # seconds
        
    def register_handler(self, dataset: str, handler: Callable[[Dict[str, Any]], None]):
        """Register a handler function for a specific dataset."""
        self.event_handlers[dataset] = handler
        logger.info(f"Registered handler for dataset: {dataset}")
    
    def connect(self) -> bool:
        """Establish connection to the streaming server."""
        try:
            self.connection = socket.create_connection((self.host, self.port))
            logger.info(f"Connected to streaming server at {self.host}:{self.port}")
            self.reconnect_attempts = 0
            return True
        except Exception as e:
            logger.error(f"Failed to connect to server: {e}")
            return False
    
    def disconnect(self):
        """Close connection to the streaming server."""
        self.running = False
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
            self.connection = None
        logger.info("Disconnected from streaming server")
    
    def _read_events(self) -> Iterator[Dict[str, Any]]:
        """Read events from the streaming connection."""
        if not self.connection:
            return
            
        try:
            with self.connection.makefile("r", encoding="utf-8") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                        yield event
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON line: {e}")
                        continue
        except Exception as e:
            logger.error(f"Error reading from stream: {e}")
            raise
    
    def _process_event(self, event: Dict[str, Any]):
        """Process a single event and route to appropriate handler."""
        try:
            # Extract dataset name
            dataset = event.get("dataset")
            if not dataset:
                logger.warning("Event missing dataset field")
                return
            
            # Add processing timestamp
            event["processed_at"] = datetime.now().isoformat()
            
            # Route to handler if registered
            if dataset in self.event_handlers:
                try:
                    self.event_handlers[dataset](event)
                except Exception as e:
                    logger.error(f"Error in handler for {dataset}: {e}")
            else:
                logger.debug(f"No handler registered for dataset: {dataset}")
                
        except Exception as e:
            logger.error(f"Error processing event: {e}")
    
    def start(self) -> bool:
        """Start the streaming client and begin processing events."""
        if not self.connect():
            return False
        
        self.running = True
        logger.info("Starting event processing...")
        
        try:
            banner_received = False
            for event in self._read_events():
                if not self.running:
                    break
                
                # First message is the banner
                if not banner_received:
                    logger.info(f"Received banner: {event}")
                    banner_received = True
                    continue
                
                # Process data events
                self._process_event(event)
                
        except Exception as e:
            logger.error(f"Error in event processing loop: {e}")
            if self.running and self.reconnect_attempts < self.max_reconnect_attempts:
                logger.info(f"Attempting to reconnect (attempt {self.reconnect_attempts + 1})")
                self.reconnect_attempts += 1
                time.sleep(self.reconnect_delay)
                self.disconnect()
                return self.start()  # Recursive reconnect
            else:
                logger.error("Max reconnect attempts reached or client stopped")
                return False
        
        return True
    
    def start_async(self) -> threading.Thread:
        """Start the client in a separate thread."""
        def run():
            self.start()
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        logger.info("Started streaming client in background thread")
        return thread
    
    def stop(self):
        """Stop the streaming client."""
        logger.info("Stopping streaming client...")
        self.running = False
        self.disconnect()


def create_client(host: str = "127.0.0.1", port: int = 8765) -> StreamingClient:
    """Factory function to create a configured streaming client."""
    return StreamingClient(host, port)


if __name__ == "__main__":
    # Test the client
    logging.basicConfig(level=logging.INFO)
    
    client = create_client()
    
    # Test handler
    def test_handler(event):
        dataset = event.get("dataset")
        sequence = event.get("sequence")
        timestamp = event.get("timestamp")
        print(f"[{dataset}] Seq: {sequence}, Time: {timestamp}")
    
    # Register test handlers
    datasets = ["Current_inventory_data", "POS_Transactions", "Product_recognism", 
                "Queue_monitor", "RFID_data"]
    
    for dataset in datasets:
        client.register_handler(dataset, test_handler)
    
    try:
        client.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        client.stop()