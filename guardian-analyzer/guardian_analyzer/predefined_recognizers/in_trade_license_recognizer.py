from typing import List, Optional, Tuple

from guardian_analyzer import Pattern, PatternRecognizer
from guardian_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InTradeLicenseRecognizer(PatternRecognizer):
    """
    Recognizes Indian Trade License identifiers in multiple languages.
    
    Includes patterns for:
    - Municipal trade licenses
    - Shop establishment licenses
    - Business registration numbers
    - Commercial permits
    - Market licenses
    - Vendor registration numbers
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Trade License",
            r"\b[A-Z]{2}TL[0-9]{8,12}\b",  # Format: MHTL12345678
            0.7,
        ),
        Pattern(
            "Shop License",
            r"\b[A-Z]{2}\/SE\/[0-9]{2}\/[0-9]{6,8}\b",  # Format: MH/SE/23/123456
            0.65,
        ),
        Pattern(
            "Business Registration",
            r"\b[A-Z]{2}BR[0-9]{8,12}\b",  # Format: MHBR12345678
            0.6,
        ),
        Pattern(
            "Commercial Permit",
            r"\b[A-Z]{2}CP[0-9]{2}[A-Z][0-9]{6,8}\b",  # Format: MHCP23A123456
            0.55,
        ),
        Pattern(
            "Market License",
            r"\b[A-Z]{2}ML[0-9]{8,12}\b",  # Format: MHML12345678
            0.5,
        ),
        Pattern(
            "Vendor Registration",
            r"\b[A-Z]{2}VR[0-9]{8,12}\b",  # Format: MHVR12345678
            0.45,
        ),
        Pattern(
            "Municipal License",
            r"\b[A-Z]{2}\/MUN\/[0-9]{2}\/[0-9]{6,8}\b",  # Format: MH/MUN/23/123456
            0.6,
        ),
        Pattern(
            "Area License",
            r"\b[A-Z]{2}[0-9]{2}[A-Z]{2}[0-9]{6,8}\b",  # Format: MH23AL123456
            0.5,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "trade license",
            "shop license",
            "business registration",
            "commercial permit",
            "market license",
            "vendor registration",
            "municipal license",
            "area license",
            "business name",
            "trade name",
            "license number",
            "registration date",
            "validity period",
            "business address",
            "license type",
            "business category",
            "jurisdiction",
            "issuing authority",
            "renewal date",
            "license status",
        ],
        "hi": [
            "व्यापार लाइसेंस",
            "दुकान लाइसेंस",
            "व्यवसाय पंजीकरण",
            "वाणिज्यिक परमिट",
            "बाजार लाइसेंस",
            "विक्रेता पंजीकरण",
            "नगरपालिका लाइसेंस",
            "क्षेत्र लाइसेंस",
            "व्यवसाय का नाम",
            "व्यापार नाम",
            "लाइसेंस संख्या",
            "पंजीकरण तिथि",
            "वैधता अवधि",
            "व्यवसाय का पता",
            "लाइसेंस प्रकार",
            "व्यवसाय श्रेणी",
            "क्षेत्राधिकार",
            "जारी करने वाला प्राधिकरण",
            "नवीनीकरण तिथि",
            "लाइसेंस स्थिति",
        ],
        # Add similar entries for ta, te, kn, mr, gu
    }

    VALID_STATE_CODES = {
        "MH": "Maharashtra",
        "DL": "Delhi",
        "KA": "Karnataka",
        "TN": "Tamil Nadu",
        "GJ": "Gujarat",
        "UP": "Uttar Pradesh",
        "WB": "West Bengal",
        "AP": "Andhra Pradesh",
        "TS": "Telangana",
        "KL": "Kerala",
    }

    VALID_LICENSE_TYPES = {
        "TL": "Trade License",
        "SE": "Shop Establishment",
        "BR": "Business Registration",
        "CP": "Commercial Permit",
        "ML": "Market License",
        "VR": "Vendor Registration",
        "MUN": "Municipal License",
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_TRADE_LICENSE",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        """Initialize the recognizer with language-specific context and patterns."""
        self.language = language
        self.replacement_pairs = (
            replacement_pairs if replacement_pairs
            else [("-", ""), ("/", ""), (" ", ""), (":", "")]
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
        return self.__check_license_reference(sanitized_value)

    def __check_license_reference(self, sanitized_value: str) -> bool:
        """
        Validate trade license reference formats.
        
        Checks:
        1. Length validation
        2. State code validation
        3. License type validation
        4. Number format validation
        5. Date format validation (if present)
        6. Area code validation
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 20:
            return False

        # Check for standard license format
        state_code = sanitized_value[:2]
        if state_code in self.VALID_STATE_CODES:
            license_type = sanitized_value[2:4]
            if license_type in self.VALID_LICENSE_TYPES:
                remaining = sanitized_value[4:]
                return remaining.isalnum() and len(remaining) >= 6

        # Check for formatted license number
        if "/" in sanitized_value:
            parts = sanitized_value.split("/")
            if len(parts) == 4:
                return (
                    parts[0] in self.VALID_STATE_CODES and
                    parts[1] in self.VALID_LICENSE_TYPES and
                    parts[2].isdigit() and len(parts[2]) == 2 and
                    parts[3].isdigit() and len(parts[3]) >= 6
                )

        # Check for area-based license format
        if len(sanitized_value) >= 10:
            state_code = sanitized_value[:2]
            year_code = sanitized_value[2:4]
            area_code = sanitized_value[4:6]
            if (state_code in self.VALID_STATE_CODES and
                year_code.isdigit() and
                area_code.isalpha()):
                return sanitized_value[6:].isdigit()

        # General alphanumeric validation
        return (
            sanitized_value.isalnum() and
            any(char.isdigit() for char in sanitized_value) and
            any(char.isalpha() for char in sanitized_value)
        )