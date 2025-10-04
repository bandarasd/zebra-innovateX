#!/usr/bin/env python3
"""
Data Parsers for Project Sentinel
Handles parsing and validation of all data streams and reference files.
"""

import csv
import json
import logging
import io
from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path


logger = logging.getLogger(__name__)


class DataParser:
    """Base parser for common functionality."""
    
    @staticmethod
    def parse_timestamp(timestamp_str: str) -> datetime:
        """Parse ISO timestamp string to datetime object."""
        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError as e:
            logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return datetime.now()
    
    @staticmethod
    def validate_required_fields(data: Dict[str, Any], required_fields: List[str], 
                                  data_type: str) -> bool:
        """Validate that all required fields are present in data."""
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            logger.warning(f"{data_type} missing required fields: {missing_fields}")
            return False
        return True


class InventoryParser(DataParser):
    """Parser for inventory snapshot data."""
    
    @staticmethod
    def parse(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse inventory snapshot event."""
        try:
            event_data = event.get("event", {})
            
            if not DataParser.validate_required_fields(
                event_data, ["timestamp", "data"], "Inventory event"
            ):
                return None
            
            timestamp = DataParser.parse_timestamp(event_data["timestamp"])
            inventory_data = event_data["data"]
            
            # Validate inventory data format
            if not isinstance(inventory_data, dict):
                logger.warning("Inventory data is not a dictionary")
                return None
            
            # Validate SKU format and quantities
            validated_inventory = {}
            for sku, quantity in inventory_data.items():
                if not isinstance(sku, str) or not sku.startswith("PRD_"):
                    logger.warning(f"Invalid SKU format: {sku}")
                    continue
                
                try:
                    quantity = int(quantity)
                    if quantity < 0:
                        logger.warning(f"Negative quantity for {sku}: {quantity}")
                        continue
                    validated_inventory[sku] = quantity
                except (ValueError, TypeError):
                    logger.warning(f"Invalid quantity for {sku}: {quantity}")
                    continue
            
            return {
                "timestamp": timestamp,
                "type": "inventory_snapshot",
                "data": validated_inventory,
                "sequence": event.get("sequence"),
                "original_timestamp": event_data["timestamp"]
            }
            
        except Exception as e:
            logger.error(f"Error parsing inventory event: {e}")
            return None


class POSTransactionParser(DataParser):
    """Parser for POS transaction data."""
    
    @staticmethod
    def parse(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse POS transaction event."""
        try:
            event_data = event.get("event", {})
            
            required_fields = ["timestamp", "station_id", "status", "data"]
            if not DataParser.validate_required_fields(event_data, required_fields, "POS event"):
                return None
            
            timestamp = DataParser.parse_timestamp(event_data["timestamp"])
            station_id = event_data["station_id"]
            status = event_data["status"]
            data = event_data["data"]
            
            # Validate transaction data
            transaction_fields = ["customer_id", "sku", "product_name", "barcode", "price", "weight_g"]
            if not DataParser.validate_required_fields(data, transaction_fields, "POS transaction data"):
                return None
            
            # Validate and convert numeric fields
            try:
                price = float(data["price"])
                weight = float(data["weight_g"])
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid numeric values in POS transaction: {e}")
                return None
            
            return {
                "timestamp": timestamp,
                "type": "pos_transaction",
                "station_id": station_id,
                "status": status,
                "customer_id": data["customer_id"],
                "sku": data["sku"],
                "product_name": data["product_name"],
                "barcode": data["barcode"],
                "price": price,
                "weight_g": weight,
                "sequence": event.get("sequence"),
                "original_timestamp": event_data["timestamp"]
            }
            
        except Exception as e:
            logger.error(f"Error parsing POS transaction event: {e}")
            return None


class RFIDParser(DataParser):
    """Parser for RFID reading data."""
    
    @staticmethod
    def parse(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse RFID reading event."""
        try:
            event_data = event.get("event", {})
            
            required_fields = ["timestamp", "station_id", "status", "data"]
            if not DataParser.validate_required_fields(event_data, required_fields, "RFID event"):
                return None
            
            timestamp = DataParser.parse_timestamp(event_data["timestamp"])
            station_id = event_data["station_id"]
            status = event_data["status"]
            data = event_data["data"]
            
            # RFID data may have null values when no tag is detected
            epc = data.get("epc")
            location = data.get("location")
            sku = data.get("sku")
            
            return {
                "timestamp": timestamp,
                "type": "rfid_reading",
                "station_id": station_id,
                "status": status,
                "epc": epc,
                "location": location,
                "sku": sku,
                "sequence": event.get("sequence"),
                "original_timestamp": event_data["timestamp"]
            }
            
        except Exception as e:
            logger.error(f"Error parsing RFID event: {e}")
            return None


class QueueMonitorParser(DataParser):
    """Parser for queue monitoring data."""
    
    @staticmethod
    def parse(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse queue monitoring event."""
        try:
            event_data = event.get("event", {})
            
            required_fields = ["timestamp", "station_id", "status", "data"]
            if not DataParser.validate_required_fields(event_data, required_fields, "Queue event"):
                return None
            
            timestamp = DataParser.parse_timestamp(event_data["timestamp"])
            station_id = event_data["station_id"]
            status = event_data["status"]
            data = event_data["data"]
            
            # Validate queue data
            queue_fields = ["customer_count", "average_dwell_time"]
            if not DataParser.validate_required_fields(data, queue_fields, "Queue data"):
                return None
            
            try:
                customer_count = int(data["customer_count"])
                avg_dwell_time = float(data["average_dwell_time"])
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid numeric values in queue data: {e}")
                return None
            
            return {
                "timestamp": timestamp,
                "type": "queue_monitoring",
                "station_id": station_id,
                "status": status,
                "customer_count": customer_count,
                "average_dwell_time": avg_dwell_time,
                "sequence": event.get("sequence"),
                "original_timestamp": event_data["timestamp"]
            }
            
        except Exception as e:
            logger.error(f"Error parsing queue monitoring event: {e}")
            return None


class ProductRecognitionParser(DataParser):
    """Parser for product recognition data."""
    
    @staticmethod
    def parse(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse product recognition event."""
        try:
            event_data = event.get("event", {})
            
            required_fields = ["timestamp", "station_id", "status", "data"]
            if not DataParser.validate_required_fields(event_data, required_fields, "Product recognition event"):
                return None
            
            timestamp = DataParser.parse_timestamp(event_data["timestamp"])
            station_id = event_data["station_id"]
            status = event_data["status"]
            data = event_data["data"]
            
            # Validate recognition data
            recognition_fields = ["predicted_product", "accuracy"]
            if not DataParser.validate_required_fields(data, recognition_fields, "Product recognition data"):
                return None
            
            try:
                accuracy = float(data["accuracy"])
                if not 0.0 <= accuracy <= 1.0:
                    logger.warning(f"Accuracy out of range [0,1]: {accuracy}")
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid accuracy value: {e}")
                return None
            
            return {
                "timestamp": timestamp,
                "type": "product_recognition",
                "station_id": station_id,
                "status": status,
                "predicted_product": data["predicted_product"],
                "accuracy": accuracy,
                "sequence": event.get("sequence"),
                "original_timestamp": event_data["timestamp"]
            }
            
        except Exception as e:
            logger.error(f"Error parsing product recognition event: {e}")
            return None


class ReferenceDataLoader:
    """Loader for CSV reference data."""
    
    @staticmethod
    def load_products(file_path: str) -> Dict[str, Dict[str, Any]]:
        """Load product catalog from CSV."""
        products = {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Skip empty lines and read CSV
                content = f.read().strip()
                lines = [line for line in content.split('\n') if line.strip()]
                
                # Create CSV reader from cleaned lines
                import io
                csv_content = '\n'.join(lines)
                reader = csv.DictReader(io.StringIO(csv_content))
                
                for row in reader:
                    if 'SKU' not in row or not row['SKU'].strip():
                        continue  # Skip rows without SKU
                    
                    sku = row['SKU'].strip()
                    products[sku] = {
                        'product_name': row['product_name'].strip(),
                        'quantity': int(row['quantity']),
                        'epc_range': row['EPC_range'].strip(),
                        'barcode': row['barcode'].strip(),
                        'weight': float(row['weight']),
                        'price': float(row['price'])
                    }
            logger.info(f"Loaded {len(products)} products from {file_path}")
        except Exception as e:
            logger.error(f"Error loading products from {file_path}: {e}")
        
        return products
    
    @staticmethod
    def load_customers(file_path: str) -> Dict[str, Dict[str, Any]]:
        """Load customer data from CSV."""
        customers = {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    customer_id = row['Customer_ID'].strip()
                    customers[customer_id] = {
                        'name': row['Name'].strip(),
                        'age': int(row['Age']),
                        'address': row['Address'].strip(),
                        'phone': row['TP'].strip()
                    }
            logger.info(f"Loaded {len(customers)} customers from {file_path}")
        except Exception as e:
            logger.error(f"Error loading customers from {file_path}: {e}")
        
        return customers


class StreamDataParser:
    """Main parser that routes events to appropriate parsers."""
    
    def __init__(self):
        self.parsers = {
            "Current_inventory_data": InventoryParser.parse,
            "POS_Transactions": POSTransactionParser.parse,
            "RFID_data": RFIDParser.parse,
            "Queue_monitor": QueueMonitorParser.parse,
            "Product_recognism": ProductRecognitionParser.parse
        }
    
    def parse_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse an event based on its dataset type."""
        dataset = event.get("dataset")
        if dataset not in self.parsers:
            logger.warning(f"No parser for dataset: {dataset}")
            return None
        
        return self.parsers[dataset](event)


if __name__ == "__main__":
    # Test the parsers
    logging.basicConfig(level=logging.INFO)
    
    # Test reference data loading
    loader = ReferenceDataLoader()
    
    # Assuming the reference files are in the data/input directory
    products = loader.load_products("../data/input/products_list.csv")
    customers = loader.load_customers("../data/input/customer_data.csv")
    
    print(f"Loaded {len(products)} products and {len(customers)} customers")