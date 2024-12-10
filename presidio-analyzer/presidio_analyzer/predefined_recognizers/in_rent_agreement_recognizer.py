from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class RentAgreementRecognizer(PatternRecognizer):
    """
    Recognizes Rent Agreement Numbers/IDs in multiple languages.
    
    Rent Agreement numbers typically contain a combination of:
    - Year
    - Registration number
    - Location code
    - Document type identifier
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Rent Agreement (Weak)",
            r"\b(RA|RG|RE)-?\d{2}/\d{4}/\d{4,8}\b",  # Basic format: RA-23/2024/12345
            0.2,
        ),
        Pattern(
            "Rent Agreement (Strong)",
            r"\b(RENT|RA|RG)\d{2}[A-Z]{2}\d{8}\b",  # Stricter format: RENT23MH12345678
            0.5,
        ),
    ]

    CONTEXT_MAP = {
        "en": ["rent agreement", "lease agreement", "rental contract", "tenancy agreement", "rent deed"],
        "hi": ["किराया समझौता", "लीज एग्रीमेंट", "किरायानामा"],  # Hindi
        "ta": ["வாடகை ஒப்பந்தம்", "குத்தகை ஒப்பந்தம்"],  # Tamil
        "te": ["అద్దె ఒప్పందం", "లీజు ఒప్పందం"],  # Telugu
        "kn": ["ಬಾಡಿಗೆ ಒಪ್ಪಂದ", "ಗುತ್ತಿಗೆ ಒಪ್ಪಂದ"],  # Kannada
        "mr": ["भाडेकरार", "लीज करार"],  # Marathi
        "gu": ["ભાડા કરાર", "લીઝ એગ્રીમેન્ટ"],  # Gujarati
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "RENT_AGREEMENT",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        """
        Initialize the recognizer with language-specific context and patterns.
        """
        self.language = language
        self.replacement_pairs = (
            replacement_pairs
            if replacement_pairs
            else [("-", ""), ("/", ""), (" ", "")]
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
        Validate the detected rent agreement number.
        """
        sanitized_value = Utils.sanitize_value(pattern_text, self.replacement_pairs)
        return self.__check_agreement_number(sanitized_value)

    def __check_agreement_number(self, sanitized_value: str) -> bool:
        """
        Perform validation checks for rent agreement numbers:
        - Length between 12 and 20 characters after sanitization
        - Starts with valid prefixes (RA, RG, RE, RENT)
        - Contains proper year format
        """
        if len(sanitized_value) < 12 or len(sanitized_value) > 20:
            return False

        # Check for valid prefixes
        valid_prefixes = ["RA", "RG", "RE", "RENT"]
        has_valid_prefix = any(sanitized_value.startswith(prefix) for prefix in valid_prefixes)
        
        # Check if the remaining part contains at least one number
        has_numbers = any(char.isdigit() for char in sanitized_value[2:])

        return has_valid_prefix and has_numbers