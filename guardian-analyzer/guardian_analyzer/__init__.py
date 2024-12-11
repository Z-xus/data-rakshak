# isort: skip_file
"""Presidio analyzer package."""

import logging

from guardian_analyzer.analysis_explanation import AnalysisExplanation
from guardian_analyzer.recognizer_result import RecognizerResult
from guardian_analyzer.dict_analyzer_result import DictAnalyzerResult
from guardian_analyzer.entity_recognizer import EntityRecognizer
from guardian_analyzer.local_recognizer import LocalRecognizer
from guardian_analyzer.pattern import Pattern
from guardian_analyzer.pattern_recognizer import PatternRecognizer
from guardian_analyzer.remote_recognizer import RemoteRecognizer
from guardian_analyzer.recognizer_registry import RecognizerRegistry
from guardian_analyzer.analyzer_engine import AnalyzerEngine
from guardian_analyzer.batch_analyzer_engine import BatchAnalyzerEngine
from guardian_analyzer.analyzer_request import AnalyzerRequest
from guardian_analyzer.analyzer_utils import PresidioAnalyzerUtils
from guardian_analyzer.context_aware_enhancers import ContextAwareEnhancer
from guardian_analyzer.context_aware_enhancers import LemmaContextAwareEnhancer
from guardian_analyzer.analyzer_engine_provider import AnalyzerEngineProvider

# Define default loggers behavior

# 1. guardian_analyzer logger

logging.getLogger("guardian-analyzer").addHandler(logging.NullHandler())

# 2. decision_process logger.
# Setting the decision process trace here as we would want it
# to be activated using a parameter to AnalyzeEngine and not by default.

decision_process_logger = logging.getLogger("decision_process")
ch = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s][%(name)s][%(levelname)s]%(message)s")
ch.setFormatter(formatter)
decision_process_logger.addHandler(ch)
decision_process_logger.setLevel("INFO")
__all__ = [
    "Pattern",
    "AnalysisExplanation",
    "RecognizerResult",
    "DictAnalyzerResult",
    "EntityRecognizer",
    "LocalRecognizer",
    "PatternRecognizer",
    "RemoteRecognizer",
    "RecognizerRegistry",
    "AnalyzerEngine",
    "AnalyzerRequest",
    "ContextAwareEnhancer",
    "LemmaContextAwareEnhancer",
    "BatchAnalyzerEngine",
    "PresidioAnalyzerUtils",
    "AnalyzerEngineProvider",
]
