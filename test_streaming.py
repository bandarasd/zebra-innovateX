#!/usr/bin/env python3
"""
Test TCP connection to streaming server
"""

import socket
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from streaming.client import StreamingClient


def test_tcp_connection():
    """Test basic TCP connection to streaming server."""
    host = "127.0.0.1"
    port = 8765
    
    print(f"Testing TCP connection to {host}:{port}")
    
    try:
        # Test basic socket connection
        with socket.create_connection((host, port), timeout=5) as sock:
            print("✓ TCP connection successful")
            
            # Read banner
            with sock.makefile("r", encoding="utf-8") as stream:
                banner_line = stream.readline()
                if banner_line:
                    banner = json.loads(banner_line)
                    print(f"✓ Received banner: {banner.get('service', 'Unknown')}")
                    print(f"  Datasets: {', '.join(banner.get('datasets', []))}")
                    print(f"  Events: {banner.get('events', 0)}")
                    
                    # Read a few events
                    print("\nFirst 3 events:")
                    for i in range(3):
                        line = stream.readline()
                        if line:
                            event = json.loads(line)
                            dataset = event.get('dataset', 'Unknown')
                            sequence = event.get('sequence', 0)
                            timestamp = event.get('timestamp', 'Unknown')
                            print(f"  [{i+1}] {dataset} - Seq: {sequence} - {timestamp}")
                        else:
                            break
                else:
                    print("✗ No banner received")
                    return False
        
        return True
        
    except ConnectionRefusedError:
        print(f"✗ Connection refused to {host}:{port}")
        print("  Make sure the streaming server is running:")
        print(f"  cd data/streaming-server && python3 stream_server.py --port {port} --speed 10 --loop")
        return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False


def test_streaming_client():
    """Test the streaming client."""
    print("\nTesting StreamingClient...")
    
    client = StreamingClient("127.0.0.1", 8765)
    
    # Test handler
    events_received = []
    
    def test_handler(event):
        events_received.append(event)
        dataset = event.get('dataset', 'Unknown')
        sequence = event.get('sequence', 0)
        print(f"  Received: {dataset} - Seq: {sequence}")
        
        # Stop after 5 events
        if len(events_received) >= 5:
            client.stop()
    
    # Register handlers for all datasets
    datasets = ["Current_inventory_data", "POS_Transactions", "Product_recognism", 
                "Queue_monitor", "RFID_data"]
    
    for dataset in datasets:
        client.register_handler(dataset, test_handler)
    
    # Test connection
    if client.connect():
        print("✓ Client connected successfully")
        client.start()
        print(f"✓ Received {len(events_received)} events")
        return True
    else:
        print("✗ Client connection failed")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("PROJECT SENTINEL STREAMING TEST")
    print("=" * 60)
    
    # Test 1: Basic TCP connection
    tcp_success = test_tcp_connection()
    
    if tcp_success:
        # Test 2: Streaming client
        client_success = test_streaming_client()
        
        if client_success:
            print("\n" + "=" * 60)
            print("✓ ALL STREAMING TESTS PASSED!")
            print("The streaming system is working correctly.")
            print("Data is available via TCP socket on 127.0.0.1:8765")
            print("=" * 60)
        else:
            print("\n✗ Streaming client test failed")
    else:
        print("\n" + "=" * 60)
        print("STREAMING SERVER NOT RUNNING")
        print("Please start the server with:")
        print("cd data/streaming-server")
        print("python3 stream_server.py --port 8765 --speed 10 --loop")
        print("=" * 60)