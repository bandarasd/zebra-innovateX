# src/algorithms/__init__.py
"""Detection and Business Intelligence Algorithms"""

from .detection import DetectionAlgorithms, CorrelationEngine
from .business_intelligence import (
    LossPreventionMetrics,
    CustomerRiskScoring,
    StationHealthMonitor,
    TrendAnalyzer,
    BusinessIntelligenceDashboard
)
from .prediction import (
    PredictiveStaffing,
    DemandForecaster,
    PerformancePredictor
)

__all__ = [
    'DetectionAlgorithms',
    'CorrelationEngine',
    'LossPreventionMetrics',
    'CustomerRiskScoring',
    'StationHealthMonitor',
    'TrendAnalyzer',
    'BusinessIntelligenceDashboard',
    'PredictiveStaffing',
    'DemandForecaster',
    'PerformancePredictor'
]
