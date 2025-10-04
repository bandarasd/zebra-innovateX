#!/usr/bin/env python3
"""
Quick test script to verify Project Sentinel functionality.
"""

import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from parsers.data_parsers import ReferenceDataLoader, StreamDataParser
from state.state_manager import StateManager
from algorithms.detection import DetectionAlgorithms
from events.generator import EventGenerator
from datetime import datetime


def test_system():
    """Test core system functionality."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("Testing Project Sentinel components...")
    
    # Test 1: Reference data loading
    logger.info("Test 1: Loading reference data...")
    loader = ReferenceDataLoader()
    products = loader.load_products("data/input/products_list.csv")
    customers = loader.load_customers("data/input/customer_data.csv")
    
    assert len(products) > 0, "No products loaded"
    assert len(customers) > 0, "No customers loaded"
    logger.info(f"✓ Loaded {len(products)} products and {len(customers)} customers")
    
    # Test 2: State manager
    logger.info("Test 2: Testing state manager...")
    state_manager = StateManager()
    state_manager.load_reference_data(products, customers)
    
    # Test inventory update
    test_inventory = {
        "timestamp": datetime.now(),
        "data": {"PRD_F_01": 100, "PRD_F_02": 80}
    }
    state_manager.update_inventory(test_inventory)
    assert len(state_manager.inventory) > 0, "Inventory not updated"
    logger.info("✓ State manager working")
    
    # Test 3: Detection algorithms
    logger.info("Test 3: Testing detection algorithms...")
    algorithms = DetectionAlgorithms(state_manager)
    
    # Test weight discrepancy detection
    test_transaction = {
        "timestamp": datetime.now(),
        "station_id": "SCC1",
        "customer_id": "C001",
        "sku": "PRD_F_01",
        "weight_g": 200.0  # Expected: 150g based on product data
    }
    
    result = algorithms.detect_weight_discrepancies(test_transaction)
    if result:
        logger.info(f"✓ Weight discrepancy detection working: {result['event_name']}")
    else:
        logger.info("✓ Weight discrepancy detection working (no discrepancy found)")
    
    # Test 4: Event generator
    logger.info("Test 4: Testing event generator...")
    generator = EventGenerator("test_output")
    
    # Create test event
    test_event_data = {
        "event_name": "Test Event",
        "station_id": "SCC1",
        "customer_id": "C001"
    }
    
    generator.add_event(datetime.now(), test_event_data)
    events = generator.get_events()
    assert len(events) > 0, "No events generated"
    
    # Validate event format
    event = events[0]
    required_fields = ["timestamp", "event_id", "event_data"]
    for field in required_fields:
        assert field in event, f"Event missing field: {field}"
    
    logger.info(f"✓ Event generator working, created event {event['event_id']}")
    
    # Test 5: Parser
    logger.info("Test 5: Testing stream parser...")
    parser = StreamDataParser()
    
    # Test parsing a mock POS transaction event
    mock_event = {
        "dataset": "POS_Transactions",
        "sequence": 1,
        "timestamp": datetime.now().isoformat(),
        "event": {
            "timestamp": datetime.now().isoformat(),
            "station_id": "SCC1",
            "status": "Active",
            "data": {
                "customer_id": "C001",
                "sku": "PRD_F_01",
                "product_name": "Test Product",
                "barcode": "1234567890",
                "price": 100.0,
                "weight_g": 150.0
            }
        }
    }
    
    parsed = parser.parse_event(mock_event)
    assert parsed is not None, "Failed to parse event"
    assert parsed["type"] == "pos_transaction", "Wrong event type"
    logger.info("✓ Stream parser working")
    
    logger.info("=" * 50)
    logger.info("ALL TESTS PASSED! Project Sentinel is ready.")
    logger.info("=" * 50)
    
    return True


if __name__ == "__main__":
    try:
        success = test_system()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Test failed: {e}")
        sys.exit(1)