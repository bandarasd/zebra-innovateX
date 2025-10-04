# Submission Guide

Complete this template before zipping your submission. Keep the file at the
project root.

## Team details
- Team name: Team##_sentinel (replace ## with your team ID)
- Members: [Add team member names here]
- Primary contact email: [Add primary contact email]

## Judge run command
Judges will `cd evidence/executables/` and run **one command** on Ubuntu 24.04:

```
python3 run_demo.py 10
```

This command will:
1. Verify Python 3.9+ environment compatibility
2. Set up the complete environment and validate all required files
3. Start the streaming server on port 8765 with 10x speed simulation
4. Run the retail analytics system for 10 minutes (configurable)
5. Generate events.jsonl files for both test and final datasets
6. Create all outputs in `./results/` directory relative to executables
7. Perform graceful cleanup of all processes and resources

The system is completely self-contained and requires no additional setup or manual intervention.

## System Architecture Summary

**Core Components:**
- Streaming client with automatic reconnection and error handling
- Multi-stream data parsers with validation for JSONL and CSV sources
- Real-time state manager for inventory, queues, and customer sessions
- 7 detection algorithms with proper `@algorithm` tagging
- Event generator outputting required JSONL format
- Web dashboard with real-time WebSocket updates

**Detection Algorithms Implemented:**
1. Scanner Avoidance (RFID correlation)
2. Barcode Switching (product recognition correlation)
3. Weight Discrepancies (tolerance-based comparison)
4. System Crashes (status monitoring)
5. Long Queue Length (threshold-based alerts)
6. Inventory Discrepancies (system consistency checks)
7. Extended Wait Times (dwell time monitoring)

## Checklist before zipping and submitting
- Algorithms tagged with `# @algorithm Name | Purpose` comments: **✓ Yes - All 7 algorithms properly tagged**
- Evidence artefacts present in `evidence/`: **✓ Yes - Output directories and automation script ready**
- Source code complete under `src/`: **✓ Yes - Complete modular architecture with streaming, parsing, detection, state management, and dashboard components**

## Additional Features
- **Real-time correlation** across multiple data streams with configurable time windows
- **Operational insights generation** for staffing and station management recommendations
- **Thread-safe operations** for concurrent processing
- **Automatic data validation** with comprehensive error handling
- **Web dashboard** accessible at http://localhost:5000 during operation
- **Comprehensive logging** for debugging and audit trails
- **Memory-efficient design** with automatic cleanup of historical data

## Output Format Compliance
All events generated in the exact required schema:
```json
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
```

The system processes all 5 data streams (inventory, POS, RFID, queue monitoring, product recognition) plus 2 reference files (products, customers) to generate comprehensive retail analytics events.