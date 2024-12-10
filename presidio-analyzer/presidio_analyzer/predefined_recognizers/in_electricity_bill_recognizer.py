from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class ElectricityBillRecognizer(PatternRecognizer):
    """
    Recognizes Electricity Bill Consumer/Account Numbers in multiple languages.
    
    Includes common patterns for identifying electricity bill numbers used by major electricity boards.
    Supports multilingual context to improve confidence scores.
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Electricity Bill Number (Basic)",
            r"\b\d{10}\b",  # Basic 10-digit number
            0.3,
        ),
        Pattern(
            "Electricity Bill Number (With Prefix)",
            r"\b[A-Z]{2,3}\d{8,10}\b",  # Format like MH1234567890
            0.5,
        ),
        Pattern(
            "Electricity Bill Number (Complex)",
            r"\b\d{4}[/-]\d{6}\b",  # Format like 1234-567890
            0.6,
        ),
        Pattern(
            "Electricity Bill Number (K-Number)",
            r"\bK\d{7,10}\b",  # K-Number format
            0.7,
        ),
        Pattern(
            "Electricity Bill Number (Service Number)",
            r"\b\d{5,7}[/-]\d{3,4}\b",  # Service number format
            0.65,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "electricity",
            "bill",
            "consumer",
            "account",
            "connection",
            "meter",
            "service",
            "number",
            "KWH",
            "power",
            "supply",
            "utility",
        ],
        "hi": ["बिजली", "बिल", "उपभोक्ता", "खाता", "कनेक्शन", "मीटर", "सेवा", "संख्या"],  # Hindi
        "ta": ["மின்சாரம்", "பில்", "நுகர்வோர்", "கணக்கு", "இணைப்பு", "மீட்டர்", "சேவை", "எண்"],  # Tamil
        "te": ["విద్యుత్", "బిల్లు", "తరచుదారు", "ఖాతా", "కనెక్షన్", "మీటర్", "సేవ", "సంఖ్య"],  # Telugu
        "kn": ["ವಿದ್ಯುತ್", "ಬಿಲ್", "ಗ್ರಾಹಕ", "ಖಾತೆ", "ಜೋಡಣೆ", "ಮೀಟರ್", "ಸೇವೆ", "ಸಂಖ್ಯೆ"],  # Kannada
        "mr": ["वीज", "बिल", "ग्राहक", "खाते", "जोड", "मीटर", "सेवा", "क्रमांक"],  # Marathi
        "gu": ["વિદ્યુત", "બિલ", "ગ્રાહક", "ખાતું", "કનેકશન", "મીટર", "સેવા", "નંબર"],  # Gujarati
    }

    VALID_PREFIXES = {
        "MSEB",  # Maharashtra
        "BEST",  # Mumbai
        "KSEB",  # Kerala
        "TNEB",  # Tamil Nadu
        "WBSEB",  # West Bengal
        "CESC",  # Calcutta
        "BSES",  # Delhi
        "NDPL",  # Delhi
        "DHBVN",  # Haryana
        "UHBVN",  # Uttar Haryana
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "ELECTRICITY_BILL_NUMBER",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        """
        Initialize the recognizer with language-specific context and patterns.
        """
        self.language = language
        self.replacement_pairs = (
            replacement_pairs if replacement_pairs else [("-", ""), (" ", ""), ("/", "")]
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
        """Validate the pattern match."""
        sanitized_value = Utils.sanitize_value(pattern_text, self.replacement_pairs)
        return self.__check_electricity_bill_number(sanitized_value)

    def __check_electricity_bill_number(self, sanitized_value: str) -> bool:
        """
        Validate the electricity bill number format.
        
        Basic validation includes:
        1. Length check
        2. Prefix validation if present
        3. Digit validation
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 15:
            return False

        # Check for valid prefixes
        for prefix in self.VALID_PREFIXES:
            if sanitized_value.upper().startswith(prefix):
                remaining = sanitized_value[len(prefix):]
                return remaining.isdigit() and len(remaining) >= 6

        # If no prefix, check if it's a valid numeric format
        if sanitized_value.startswith("K"):
            return sanitized_value[1:].isdigit() and len(sanitized_value) >= 8

        # Check if it's all digits
        if sanitized_value.isdigit():
            return len(sanitized_value) >= 8

        return False
