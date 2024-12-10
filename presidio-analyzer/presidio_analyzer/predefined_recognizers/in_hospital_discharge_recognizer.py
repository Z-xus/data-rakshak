from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InHospitalDischargeRecognizer(PatternRecognizer):
    """
    Recognizes Indian Hospital Discharge Summary identifiers in multiple languages.
    
    Includes patterns for:
    - Discharge summary numbers
    - Patient IDs
    - Admission numbers
    - Case reference numbers
    - Hospital registration numbers
    - Medical record numbers
    - Treatment IDs
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Discharge Summary Number",
            r"\b(DS|DIS)[A-Z0-9]{8,12}\b",  # Format: DS12345678
            0.6,
        ),
        Pattern(
            "Patient ID",
            r"\b(PT|PID)[A-Z0-9]{6,10}\b",  # Format: PT123456
            0.5,
        ),
        Pattern(
            "Admission Number",
            r"\b(ADM|IP)[A-Z0-9]{8,12}\b",  # Format: ADM12345678
            0.65,
        ),
        Pattern(
            "Case Reference",
            r"\b(CR|CASE)[A-Z0-9]{6,10}\b",  # Format: CR123456
            0.55,
        ),
        Pattern(
            "Hospital Registration",
            r"\b[A-Z]{2,4}\/REG\/[0-9]{6,10}\b",  # Format: ABC/REG/123456
            0.7,
        ),
        Pattern(
            "Medical Record Number",
            r"\b(MR|MRN)[A-Z0-9]{6,10}\b",  # Format: MR123456
            0.6,
        ),
        Pattern(
            "Treatment ID",
            r"\b(TR|TRT)[A-Z0-9]{6,10}\b",  # Format: TR123456
            0.5,
        ),
        Pattern(
            "Ward Reference",
            r"\b[A-Z]{1,3}\/[A-Z]{1,3}\/[0-9]{4,8}\b",  # Format: ICU/B/12345
            0.45,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "discharge summary",
            "patient id",
            "admission number",
            "case reference",
            "hospital registration",
            "medical record",
            "treatment id",
            "ward number",
            "admission date",
            "discharge date",
            "hospital name",
            "patient name",
            "diagnosis",
            "treatment given",
            "medications",
            "follow up",
            "doctor name",
            "department",
            "ward details",
            "hospital stay",
        ],
        "hi": [
            "डिस्चार्ज सारांश",
            "मरीज आईडी",
            "भर्ती संख्या",
            "केस संदर्भ",
            "अस्पताल पंजीकरण",
            "चिकित्सा रिकॉर्ड",
            "उपचार आईडी",
            "वार्ड नंबर",
            "भर्ती तिथि",
            "डिस्चार्ज तिथि",
            "अस्पताल का नाम",
            "मरीज का नाम",
            "निदान",
            "दिया गया उपचार",
            "दवाइयां",
            "आगे की जांच",
            "डॉक्टर का नाम",
            "विभाग",
            "वार्ड विवरण",
            "अस्पताल में ठहरने की अवधि",
        ],
        "ta": [
            "டிஸ்சார்ஜ் சம்மரி",
            "நோயாளி ஐடி",
            "அட்மிஷன் எண்",
            "கேஸ் குறிப்பு",
            "மருத்துவமனை பதிவு",
            "மருத்துவ பதிவு",
            "சிகிச்சை ஐடி",
            "வார்டு எண்",
            "அட்மிஷன் தேதி",
            "டிஸ்சார்ஜ் தேதி",
            "மருத்துவமனை பெயர்",
            "நோயாளி பெயர்",
            "நோய் கண்டறிதல்",
            "அளிக்கப்பட்ட சிகிச்சை",
            "மருந்துகள்",
            "தொடர் சிகிச்சை",
            "மருத்துவர் பெயர்",
            "துறை",
            "வார்டு விவரங்கள்",
            "மருத்துவமனை தங்கல்",
        ],
        # Add similar entries for te, kn, mr, gu
    }

    VALID_PREFIXES = {
        "DS",   # Discharge Summary
        "DIS",  # Discharge
        "PT",   # Patient
        "PID",  # Patient ID
        "ADM",  # Admission
        "IP",   # In-Patient
        "CR",   # Case Reference
        "MR",   # Medical Record
        "MRN",  # Medical Record Number
        "TR",   # Treatment
        "TRT",  # Treatment
    }

    VALID_DEPARTMENTS = {
        "GEN",  # General Medicine
        "SUR",  # Surgery
        "PED",  # Pediatrics
        "GYN",  # Gynecology
        "ORT",  # Orthopedics
        "ENT",  # Ear Nose Throat
        "CAR",  # Cardiology
        "NEU",  # Neurology
        "ONC",  # Oncology
        "ICU",  # Intensive Care Unit
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_HOSPITAL_DISCHARGE",
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
        return self.__check_discharge_reference(sanitized_value)

    def __check_discharge_reference(self, sanitized_value: str) -> bool:
        """
        Validate hospital discharge reference formats.
        
        Checks:
        1. Length validation
        2. Prefix validation
        3. Department code validation
        4. Number format validation
        5. Date format validation (if present)
        6. Ward reference validation
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 20:
            return False

        # Check for valid prefixes
        for prefix in self.VALID_PREFIXES:
            if sanitized_value.upper().startswith(prefix):
                remaining = sanitized_value[len(prefix):]
                return remaining.isalnum() and any(char.isdigit() for char in remaining)

        # Check for department/ward reference format
        if "/" in sanitized_value:
            parts = sanitized_value.split("/")
            if len(parts) == 3:
                return (
                    parts[0] in self.VALID_DEPARTMENTS and 
                    len(parts[1]) <= 3 and 
                    parts[2].isdigit()
                )

        # Check for date-based format (YYYYMMDD)
        if len(sanitized_value) >= 8:
            date_part = sanitized_value[-8:]
            if date_part.isdigit():
                year = int(date_part[:4])
                month = int(date_part[4:6])
                day = int(date_part[6:])
                return (
                    2000 <= year <= 2100 and 
                    1 <= month <= 12 and 
                    1 <= day <= 31
                )

        # General alphanumeric validation
        return (
            sanitized_value.isalnum() and 
            any(char.isdigit() for char in sanitized_value) and 
            any(char.isalpha() for char in sanitized_value)
        )