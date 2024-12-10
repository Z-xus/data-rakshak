from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InDrivingLicenseRecognizer(PatternRecognizer):
    """
    Recognizes Indian Driving License Numbers in multiple languages.
    
    Driving License numbers in India are alphanumeric and vary by state.
    The recognizer supports different languages by dynamically loading
    context words and patterns specific to the given language.
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Driving License (Weak)",
            r"\b[A-Z]{2}-?\d{2,4}-?\d{4,11}\b",
            0.2,
        ),
        Pattern(
            "Driving License (Strong)",
            r"\b[A-Z]{2}\d{2}-\d{4,11}\b",
            0.5,
        ),
    ]

    CONTEXT_MAP = {
        "en": ["driving license", "license number", "dl number", "driver license"],
        "hi": ["ड्राइविंग लाइसेंस", "लाइसेंस नंबर", "डीएल नंबर"],  # Hindi
        "ta": ["இயக்குநர் உரிமம்", "உரிமம் எண்", "டி.எல் எண்"],  # Tamil
        "te": ["డ్రైవింగ్ లైసెన్స్", "లైసెన్స్ నంబర్", "డి.ఎల్ నంబర్"],  # Telugu
        "kn": ["ಚಾಲನಾ ಪರವಾನಗಿ", "ಪರವಾನಗಿ ಸಂಖ್ಯೆ", "ಡಿಎಲ್ ಸಂಖ್ಯೆ"],  # Kannada
        "mr": ["वाहन चालविण्याचा परवाना", "परवाना क्रमांक", "डीएल क्रमांक"],  # Marathi
        "gu": ["ડ્રાઈવિંગ લાઇસન્સ", "લાઇસન્સ નંબર", "ડીએલ નંબર"],  # Gujarati
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_DRIVING_LICENSE",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        """
        Initialize the recognizer with language-specific context and patterns.
        """
        self.language = language
        self.replacement_pairs = (
            replacement_pairs
            if replacement_pairs
            else [("-", ""), (" ", ""), (":", "")]
        )
        patterns = patterns if patterns else self.DEFAULT_PATTERNS
        context = context if context else self.CONTEXT_MAP.get(language, [])
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=language,
        )

    def validate_result(self, pattern_text: str) -> bool:
        """
        Validate the detected license number.
        - Length between 10 and 16 characters after sanitization.
        - Starts with valid state code (two uppercase letters).
        """
        sanitized_value = Utils.sanitize_value(pattern_text, self.replacement_pairs)
        return self.__check_license(sanitized_value)

    def __check_license(self, sanitized_value: str) -> bool:
        """
        Perform additional checks for validity.
        """
        if (
            10 <= len(sanitized_value) <= 16
            and sanitized_value[:2].isalpha()
            and sanitized_value[:2].isupper()
            and sanitized_value[2:].isdigit()
        ):
            return True
        return False
