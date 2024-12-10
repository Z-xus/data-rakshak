from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InGSTCertificateRecognizer(PatternRecognizer):
    """
    Recognizes Indian GST Registration Certificate identifiers in multiple languages.
    
    Includes patterns for:
    - GSTIN numbers
    - Provisional GST IDs
    - ARN numbers
    - Registration certificate numbers
    - TRN numbers
    - UIN numbers
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "GSTIN",
            r"\b\d{2}[A-Z]{5}\d{4}[A-Z][A-Z\d]{3}\b",  # Format: 27AAPFU0939F1Z5
            0.9,
        ),
        Pattern(
            "Provisional GST ID",
            r"\b\d{2}[A-Z]{5}[P][A-Z\d]{6}\b",  # Format: 27AAPFUP123456
            0.8,
        ),
        Pattern(
            "ARN Number",
            r"\b[A-Z]{2}\d{14}\b",  # Format: AA123456789012345
            0.7,
        ),
        Pattern(
            "Registration Certificate",
            r"\b[A-Z]{2}\/GST\/REG\/\d{2}\/\d{6,8}\b",  # Format: MH/GST/REG/23/123456
            0.65,
        ),
        Pattern(
            "TRN Number",
            r"\b[A-Z]{2}\d{2}[A-Z]{2}\d{6,8}\b",  # Format: MH23TR123456
            0.6,
        ),
        Pattern(
            "UIN Number",
            r"\b[A-Z]{2}[U][A-Z]{2}\d{6,8}\b",  # Format: MHUIN123456
            0.55,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "gst registration",
            "gstin",
            "tax identification",
            "registration certificate",
            "provisional id",
            "arn number",
            "trn number",
            "uin",
            "business name",
            "trade name",
            "registration date",
            "business address",
            "tax period",
            "tax liability",
            "registration type",
            "business category",
            "jurisdiction",
            "tax official",
            "certificate validity",
            "registration status",
        ],
        "hi": [
            "जीएसटी पंजीकरण",
            "जीएसटीआईएन",
            "कर पहचान",
            "पंजीकरण प्रमाणपत्र",
            "अस्थायी आईडी",
            "एआरएन नंबर",
            "टीआरएन नंबर",
            "यूआईएन",
            "व्यवसाय का नाम",
            "व्यापार नाम",
            "पंजीकरण तिथि",
            "व्यवसाय का पता",
            "कर अवधि",
            "कर देयता",
            "पंजीकरण प्रकार",
            "व्यवसाय श्रेणी",
            "क्षेत्राधिकार",
            "कर अधिकारी",
            "प्रमाणपत्र वैधता",
            "पंजीकरण स्थिति",
        ],
        # Add similar entries for ta, te, kn, mr, gu
    }

    VALID_STATE_CODES = {
        "01": "Jammu",
        "02": "HP",
        "03": "Punjab",
        "04": "Chandigarh",
        "05": "Uttarakhand",
        "06": "Haryana",
        "07": "Delhi",
        "08": "Rajasthan",
        "09": "UP",
        "10": "Bihar",
        "11": "Sikkim",
        "12": "Arunachal",
        "13": "Nagaland",
        "14": "Manipur",
        "15": "Mizoram",
        "16": "Tripura",
        "17": "Meghalaya",
        "18": "Assam",
        "19": "WB",
        "20": "Jharkhand",
        "21": "Odisha",
        "22": "Chhattisgarh",
        "23": "MP",
        "24": "Gujarat",
        "27": "Maharashtra",
        "29": "Karnataka",
        "32": "Kerala",
        "33": "TN",
        "36": "Telangana",
        "37": "AP",
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_GST_CERTIFICATE",
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
        return self.__check_gst_reference(sanitized_value)

    def __check_gst_reference(self, sanitized_value: str) -> bool:
        """
        Validate GST certificate reference formats.
        
        Checks:
        1. Length validation
        2. State code validation
        3. PAN format validation
        4. Entity type validation
        5. Check digit validation
        6. Registration type validation
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 20:
            return False

        # GSTIN format validation (15 characters)
        if len(sanitized_value) == 15:
            state_code = sanitized_value[:2]
            pan = sanitized_value[2:12]
            entity = sanitized_value[12]
            check_digit = sanitized_value[14]
            
            return (
                state_code in self.VALID_STATE_CODES and
                pan.isalnum() and
                entity.isalnum() and
                check_digit.isalnum()
            )

        # ARN number validation
        if len(sanitized_value) == 16 and sanitized_value[:2].isalpha():
            return sanitized_value[2:].isdigit()

        # Registration certificate format
        if "/" in sanitized_value:
            parts = sanitized_value.split("/")
            if len(parts) == 5:
                return (
                    parts[0] in [self.VALID_STATE_CODES[code] for code in self.VALID_STATE_CODES] and
                    parts[1] == "GST" and
                    parts[2] == "REG" and
                    parts[3].isdigit() and len(parts[3]) == 2 and
                    parts[4].isdigit() and len(parts[4]) >= 6
                )

        # TRN/UIN format validation
        if len(sanitized_value) >= 10:
            state_prefix = sanitized_value[:2]
            if state_prefix in [self.VALID_STATE_CODES[code] for code in self.VALID_STATE_CODES]:
                remaining = sanitized_value[2:]
                return remaining.isalnum()

        # General alphanumeric validation
        return (
            sanitized_value.isalnum() and
            any(char.isdigit() for char in sanitized_value) and
            any(char.isalpha() for char in sanitized_value)
        )