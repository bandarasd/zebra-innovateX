#!/usr/bin/env python3
"""
System Verification Test for Enhanced Project Sentinel
Run this to verify all components are working correctly
"""

import sys
import json
from pathlib import Path
from datetime import datetime
from collections import Counter

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

def test_imports():
    """Test that all modules can be imported"""
    print("🔍 Testing module imports...")
    
    try:
        from state.state_manager import StateManager
        from algorithms.detection import DetectionAlgorithms
        from algorithms.business_intelligence import (
            BusinessIntelligenceDashboard,
            LossPreventionMetrics,
            CustomerRiskScoring,
            StationHealthMonitor
        )
        from algorithms.prediction import PredictiveStaffing, DemandForecaster
        from events.generator import EventGenerator
        from parsers.data_parsers import ReferenceDataLoader
        from streaming.client import StreamingClient
        
        print("✅ All modules imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False


def test_detection_algorithms():
    """Test that detection algorithms work"""
    print("\n🔍 Testing detection algorithms...")
    
    try:
        from state.state_manager import StateManager
        from algorithms.detection import DetectionAlgorithms
        
        sm = StateManager()
        sm.products = {
            "PRD_F_01": {"weight": 150.0, "price": 280.0, "quantity": 100},
            "PRD_F_02": {"weight": 200.0, "price": 350.0, "quantity": 80},
        }
        
        algorithms = DetectionAlgorithms(sm)
        
        # Test weight discrepancy
        result = algorithms.detect_weight_discrepancies({
            "timestamp": datetime.now(),
            "station_id": "SCC1",
            "customer_id": "C001",
            "sku": "PRD_F_01",
            "weight_g": 250.0
        })
        
        if result and "potential_loss" in result:
            print(f"✅ Detection algorithms working (detected weight discrepancy)")
            print(f"   Financial impact: ${result['potential_loss']:.2f}")
            return True
        else:
            print("❌ Detection algorithms not returning enhanced data")
            return False
            
    except Exception as e:
        print(f"❌ Error testing detection: {e}")
        return False


def test_business_intelligence():
    """Test BI components"""
    print("\n🔍 Testing business intelligence...")
    
    try:
        from state.state_manager import StateManager
        from algorithms.business_intelligence import (
            LossPreventionMetrics,
            CustomerRiskScoring,
            BusinessIntelligenceDashboard
        )
        
        sm = StateManager()
        sm.products = {"PRD_F_01": {"price": 280.0}}
        
        # Test loss metrics
        loss_metrics = LossPreventionMetrics(sm)
        loss_metrics.record_loss({
            "event_name": "Scanner Avoidance",
            "potential_loss": 280.0,
            "station_id": "SCC1"
        })
        
        summary = loss_metrics.get_daily_loss_summary()
        
        if summary["total_potential_loss"] == 280.0:
            print("✅ Loss prevention metrics working")
        else:
            print("❌ Loss metrics calculation incorrect")
            return False
        
        # Test risk scoring
        risk_scoring = CustomerRiskScoring(sm)
        sm.record_incident("C001", {
            "event_name": "Scanner Avoidance",
            "potential_loss": 150.0,
            "timestamp": datetime.now()
        })
        
        risk_score = risk_scoring.calculate_risk_score("C001")
        
        if risk_score > 0:
            print(f"✅ Customer risk scoring working (score: {risk_score:.1f})")
        else:
            print("❌ Risk scoring not working")
            return False
        
        # Test BI dashboard
        bi = BusinessIntelligenceDashboard(sm)
        summary = bi.get_executive_summary()
        
        if "kpis" in summary:
            print("✅ BI dashboard working")
            return True
        else:
            print("❌ BI dashboard not generating reports")
            return False
            
    except Exception as e:
        print(f"❌ Error testing BI: {e}")
        return False


def test_predictions():
    """Test predictive analytics"""
    print("\n🔍 Testing predictive analytics...")
    
    try:
        from state.state_manager import StateManager
        from algorithms.prediction import PredictiveStaffing, DemandForecaster
        
        sm = StateManager()
        
        # Test predictive staffing
        staffing = PredictiveStaffing(sm)
        staffing.record_current_state()
        
        print("✅ Predictive staffing initialized")
        
        # Test demand forecaster
        forecaster = DemandForecaster(sm)
        forecaster.track_product_sale("PRD_F_01", datetime.now())
        
        print("✅ Demand forecaster working")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing predictions: {e}")
        return False


def test_event_generator():
    """Test event generation with BI features"""
    print("\n🔍 Testing event generator...")
    
    try:
        from events.generator import EventGenerator
        
        generator = EventGenerator("test_output")
        
        # Create test event with financial data
        generator.add_event(datetime.now(), {
            "event_name": "Scanner Avoidance",
            "station_id": "SCC1",
            "customer_id": "C001",
            "product_sku": "PRD_F_01",
            "potential_loss": 280.0,
            "financial_impact": "MEDIUM"
        })
        
        stats = generator.get_event_statistics()
        
        if stats["total_events"] == 1:
            print("✅ Event generator working")
        else:
            print("❌ Event generation failed")
            return False
        
        if "financial_summary" in stats:
            print(f"✅ Financial tracking enabled (${stats['financial_summary']['total_potential_loss']:.2f})")
            return True
        else:
            print("❌ Financial tracking not working")
            return False
            
    except Exception as e:
        print(f"❌ Error testing event generator: {e}")
        return False


def test_backward_compatibility():
    """Test that original event format still works"""
    print("\n🔍 Testing backward compatibility...")
    
    try:
        from events.generator import EventGenerator
        
        generator = EventGenerator("test_output")
        
        # Original format (without financial fields)
        original_event = {
            "event_name": "Long Queue Length",
            "station_id": "SCC1",
            "num_of_customers": 6
        }
        
        generator.add_event(datetime.now(), original_event)
        events = generator.get_events()
        
        # Verify structure
        event = events[0]
        if all(key in event for key in ["timestamp", "event_id", "event_data"]):
            print("✅ Original event format still works")
            return True
        else:
            print("❌ Backward compatibility broken")
            return False
            
    except Exception as e:
        print(f"❌ Error testing compatibility: {e}")
        return False


def test_file_paths():
    """Test that required files exist"""
    print("\n🔍 Testing file structure...")
    
    project_root = Path(__file__).parent.parent
    
    required_files = [
        "data/streaming-server/stream_server.py",
        "data/streaming-clients/client_example.py",
    ]
    
    all_exist = True
    for file_path in required_files:
        full_path = project_root / file_path
        if full_path.exists():
            print(f"✅ {file_path}")
        else:
            print(f"❌ Missing: {file_path}")
            all_exist = False
    
    return all_exist


def test_sample_data():
    """Test loading sample reference data"""
    print("\n🔍 Testing reference data loading...")
    
    try:
        from parsers.data_parsers import ReferenceDataLoader
        
        project_root = Path(__file__).parent.parent
        data_dir = project_root / "data" / "input"
        
        if not data_dir.exists():
            print("❌ Data directory not found")
            return False
        
        loader = ReferenceDataLoader()
        
        # Try to load products
        products_file = data_dir / "products_list.csv"
        if products_file.exists():
            products = loader.load_products(str(products_file))
            if products:
                print(f"✅ Loaded {len(products)} products")
            else:
                print("❌ Failed to load products")
                return False
        else:
            print("⚠️  products_list.csv not found (optional for test)")
        
        # Try to load customers
        customers_file = data_dir / "customer_data.csv"
        if customers_file.exists():
            customers = loader.load_customers(str(customers_file))
            if customers:
                print(f"✅ Loaded {len(customers)} customers")
                return True
            else:
                print("❌ Failed to load customers")
                return False
        else:
            print("⚠️  customer_data.csv not found (optional for test)")
            return True
            
    except Exception as e:
        print(f"❌ Error loading reference data: {e}")
        return False


def main():
    """Run all verification tests"""
    print("=" * 60)
    print("PROJECT SENTINEL - ENHANCED SYSTEM VERIFICATION")
    print("=" * 60)
    
    tests = [
        ("Module Imports", test_imports),
        ("Detection Algorithms", test_detection_algorithms),
        ("Business Intelligence", test_business_intelligence),
        ("Predictive Analytics", test_predictions),
        ("Event Generator", test_event_generator),
        ("Backward Compatibility", test_backward_compatibility),
        ("File Structure", test_file_paths),
        ("Reference Data", test_sample_data),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ Test crashed: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status:10} {test_name}")
    
    print("=" * 60)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED - System is ready!")
        print("\n📋 Next steps:")
        print("  1. Start streaming server: cd data/streaming-server && python stream_server.py --speed 10 --loop")
        print("  2. Run detection system: cd src && python main.py batch 2")
        print("  3. Check results: cat evidence/output/test/events.jsonl")
        return 0
    else:
        print("⚠️  SOME TESTS FAILED - Review errors above")
        return 1


if __name__ == "__main__":
    sys.exit(main())
    