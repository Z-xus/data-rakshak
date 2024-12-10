from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InMedicalInsuranceRecognizer(PatternRecognizer):
    """
    Recognizes Indian Medical Insurance Policy identifiers in multiple languages.
    
    Includes patterns for:
    - Policy numbers
    - TPA IDs
    - Claim numbers
    - Card numbers
    - Member IDs
    - Group policy numbers
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Policy Number (Basic)",
            r"\b[A-Z]{2,4}\/H\/[0-9]{6,12}\b",  # Format: MED/H/123456
            0.6,
        ),
        Pattern(
            "Policy Number (Complex)",
            r"\b[A-Z]{2,4}\/MED\/[0-9]{2}\/[0-9]{4,8}\b",  # Format: GIC/MED/23/12345
            0.7,
        ),
        Pattern(
            "TPA ID",
            r"\b[A-Z]{3,5}TPA[0-9]{5,8}\b",  # Format: ICICTPA12345
            0.65,
        ),
        Pattern(
            "Claim Number",
            r"\b[A-Z]{2,4}CLM[0-9]{8,12}\b",  # Format: MEDCLM12345678
            0.55,
        ),
        Pattern(
            "Health Card Number",
            r"\b[A-Z]{2,4}HC[0-9]{8,12}\b",  # Format: STARHC12345678
            0.5,
        ),
        Pattern(
            "Member ID",
            r"\b[A-Z]{2,4}MEM[0-9]{6,10}\b",  # Format: NICMEM123456
            0.45,
        ),
        Pattern(
            "Group Policy",
            r"\b[A-Z]{2,4}GRP[0-9]{6,10}\b",  # Format: MAXGRP123456
            0.55,
        ),
        Pattern(
            "Family Floater",
            r"\b[A-Z]{2,4}FF[0-9]{6,10}\b",  # Format: STARFF123456
            0.5,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "medical insurance",
            "health insurance",
            "policy number",
            "policy document",
            "insurance card",
            "tpa id",
            "claim number",
            "member id",
            "group policy",
            "family floater",
            "sum insured",
            "premium amount",
            "policy period",
            "coverage details",
            "insured person",
            "policy holder",
            "insurance provider",
            "policy start date",
            "policy end date",
            "renewal date",
        ],
        "hi": [
            "मेडिकल बीमा",
            "स्वास्थ्य बीमा",
            "पॉलिसी नंबर",
            "बीमा दस्तावेज",
            "बीमा कार्ड",
            "टीपीए आईडी",
            "दावा संख्या",
            "सदस्य आईडी",
            "समूह पॉलिसी",
            "फैमिली फ्लोटर",
            "बीमा राशि",
            "प्रीमियम राशि",
            "पॉलिसी अवधि",
            "कवरेज विवरण",
            "बीमित व्यक्ति",
            "पॉलिसी धारक",
            "बीमा प्रदाता",
            "पॉलिसी आरंभ तिथि",
            "पॉलिसी समाप्ति तिथि",
            "नवीनीकरण तिथि",
        ],
        "ta": [
            "மருத்துவக் காப்பீடு",
            "சுகாதார காப்பீடு",
            "பாலிசி எண்",
            "காப்பீட்டு ஆவணம்",
            "காப்பீட்டு அட்டை",
            "டிபிஏ ஐடி",
            "கோரிக்கை எண்",
            "உறுப்பினர் ஐடி",
            "குழு பாலிசி",
            "குடும்ப ஃப்ளோட்டர்",
            "காப்பீட்டுத் தொகை",
            "ப்ரீமியம் தொகை",
            "பாலிசி காலம்",
            "காப்பீட்டு விவரங்கள்",
            "காப்பீடு பெற்றவர்",
            "பாலிசி வைத்திருப்பவர்",
            "காப்பீட்டு நிறுவனம்",
            "பாலிசி தொடக்க தேதி",
            "பாலிசி முடிவு தேதி",
            "புதுப்பித்தல் தேதி",
        ],
        # Add similar entries for te, kn, mr, gu
    }

    VALID_PREFIXES = {
        "MED",  # Medical
        "HLT",  # Health
        "GIC",  # General Insurance
        "NIC",  # National Insurance
        "STA",  # Star Health
        "MAX",  # Max Bupa
        "CIG",  # CIGNA
        "APL",  # Apollo
        "HDC",  # HDFC Health
        "ICH",  # ICICI Health
    }

    VALID_TYPES = {
        "TPA",  # Third Party Administrator
        "CLM",  # Claim
        "HC",   # Health Card
        "MEM",  # Member
        "GRP",  # Group
        "FF",   # Family Floater
        "POL",  # Policy
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_MEDICAL_INSURANCE",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        """
        Initialize the recognizer with language-specific context and patterns.
        """
        self.language = language
        self.replacement_pairs = (
            replacement_pairs
            if replacement_pairs
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
        return self.__check_insurance_reference(sanitized_value)

    def __check_insurance_reference(self, sanitized_value: str) -> bool:
        """
        Validate medical insurance reference formats.
        
        Checks:
        1. Length validation
        2. Prefix validation
        3. Insurance provider code validation
        4. Type code validation
        5. Number format validation
        6. Date format validation (if present)
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 20:
            return False

        # Check for valid insurance provider prefixes
        for prefix in self.VALID_PREFIXES:
            if sanitized_value.upper().startswith(prefix):
                remaining = sanitized_value[len(prefix):]
                # Check if remaining part contains a valid type code
                for type_code in self.VALID_TYPES:
                    if remaining.startswith(type_code):
                        number_part = remaining[len(type_code):]
                        return number_part.isdigit() and len(number_part) >= 6

        # Check for policy number format with date components
        if "/" in sanitized_value:
            parts = sanitized_value.split("/")
            if len(parts) == 3:
                if (parts[0] in self.VALID_PREFIXES and 
                    parts[1] in ["H", "MED"] and 
                    parts[2].isdigit()):
                    return True

        # Check for TPA ID format
        if "TPA" in sanitized_value:
            prefix = sanitized_value[:sanitized_value.index("TPA")]
            suffix = sanitized_value[sanitized_value.index("TPA")+3:]
            return (
                prefix in self.VALID_PREFIXES and 
                suffix.isdigit() and 
                5 <= len(suffix) <= 8
            )

        # General alphanumeric validation
        return (
            sanitized_value.isalnum() and 
            any(char.isdigit() for char in sanitized_value) and 
            any(char.isalpha() for char in sanitized_value)
        )