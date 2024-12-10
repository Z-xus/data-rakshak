from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InForm16TDSRecognizer(PatternRecognizer):
    """
    Recognizes Indian Form 16 and TDS-related identifiers in multiple languages.
    
    Includes patterns for:
    - Form 16 document numbers
    - TDS certificate numbers
    - TAN numbers
    - Challan numbers
    - Assessment Year references
    - Quarter references
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Form 16 Number (Basic)",
            r"\bF16[A-Z0-9]{8,12}\b",
            0.4,
        ),
        Pattern(
            "TDS Certificate Number",
            r"\b[A-Z]{3}[A-Z0-9]{7}[1-4][0-9]{4}\b",  # Format like MUMG01234911920
            0.7,
        ),
        Pattern(
            "TAN Number",
            r"\b[A-Z]{4}[0-9]{5}[A-Z]\b",  # Format like DELA12345F
            0.8,
        ),
        Pattern(
            "Challan Number",
            r"\b[0-9]{5}[A-Z][0-9]{5}[A-Z0-9]{3}\b",  # Format like 12345A12345ABC
            0.6,
        ),
        Pattern(
            "Assessment Year",
            r"\b(AY|FY)\s?20[0-9]{2}-?2[0-9]{3}\b",  # Format like AY2023-2024
            0.5,
        ),
        Pattern(
            "Quarter Reference",
            r"\bQ[1-4]\s?20[0-9]{2}-?2[0-9]{3}\b",  # Format like Q1 2023-2024
            0.45,
        ),
        Pattern(
            "Form 16 Part Reference",
            r"\bF16-?(PART|PT)-?[AB]\d{8,12}\b",  # Format like F16-PART-A12345678
            0.65,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "form 16",
            "tds certificate",
            "tax deduction",
            "tax deducted",
            "source deduction",
            "tan number",
            "challan number",
            "assessment year",
            "financial year",
            "quarter",
            "employer details",
            "employee details",
            "deductor details",
            "deductee details",
            "part a",
            "part b",
            "salary details",
            "income tax",
            "certificate number",
            "tax payment",
        ],
        "hi": [
            "फॉर्म 16",
            "टीडीएस प्रमाणपत्र",
            "कर कटौती",
            "स्रोत पर कटौती",
            "टैन नंबर",
            "चालान नंबर",
            "कर निर्धारण वर्ष",
            "वित्तीय वर्ष",
            "तिमाही",
            "नियोक्ता विवरण",
            "कर्मचारी विवरण",
            "कटौतीकर्ता विवरण",
            "कटौती विवरण",
            "भाग ए",
            "भाग बी",
            "वेतन विवरण",
            "आयकर",
            "प्रमाणपत्र संख्या",
            "कर भुगतान",
            "आय विवरण",
        ],
        "ta": [
            "படிவம் 16",
            "டிடிஎஸ் சான்றிதழ்",
            "வரி பிடித்தம்",
            "மூல பிடித்தம்",
            "டான் எண்",
            "சலான் எண்",
            "வரி மதிப்பீட்டு ஆண்டு",
            "நிதியாண்டு",
            "காலாண்டு",
            "முதலாளி விவரங்கள்",
            "பணியாளர் விவரங்கள்",
            "கழிப்பவர் விவரங்கள்",
            "கழிக்கப்படுபவர் விவரங்கள்",
            "பகுதி ஏ",
            "பகுதி பி",
            "சம்பள விவரங்கள்",
            "வருமான வரி",
            "சான்றிதழ் எண்",
            "வரி செலுத்துதல்",
            "வருமான விவரங்கள்",
        ],
        "te": [
            "ఫారం 16",
            "టిడిఎస్ సర్టిఫికేట్",
            "పన్ను మినహాయింపు",
            "మూల మినహాయింపు",
            "టాన్ నంబర్",
            "చలాన్ నంబర్",
            "అసెస్మెంట్ సంవత్సరం",
            "ఆర్థిక సంవత్సరం",
            "త్రైమాసికం",
            "యజమాని వివరాలు",
            "ఉద్యోగి వివరాలు",
            "మినహాయింపుదారు వివరాలు",
            "మినహాయింపు పొందిన వివరాలు",
            "పార్ట్ ఎ",
            "పార్ట్ బి",
            "జీతం వివరాలు",
            "ఆదాయపు పన్ను",
            "సర్టిఫికేట్ నంబర్",
            "పన్ను చెల్లింపు",
            "ఆదాయ వివరాలు",
        ],
        "kn": [
            "ನಮೂನೆ 16",
            "ಟಿಡಿಎಸ್ ಪ್ರಮಾಣಪತ್ರ",
            "ತೆರಿಗೆ ಕಡಿತ",
            "ಮೂಲದಲ್ಲಿ ಕಡಿತ",
            "ಟ್ಯಾನ್ ಸಂಖ್ಯೆ",
            "ಚಲನ್ ಸಂಖ್ಯೆ",
            "ತೆರಿಗೆ ನಿರ್ಧಾರಣೆ ವರ್ಷ",
            "ಹಣಕಾಸು ವರ್ಷ",
            "ತ್ರೈಮಾಸಿಕ",
            "ಉದ್ಯೋಗದಾತ ವಿವರಗಳು",
            "ನೌಕರ ವಿವರಗಳು",
            "ಕಡಿತಗಾರ ವಿವರಗಳು",
            "ಕಡಿತ ಹೊಂದಿದವರ ವಿವರಗಳು",
            "ಭಾಗ ಎ",
            "ಭಾಗ ಬಿ",
            "ವೇತನ ವಿವರಗಳು",
            "ಆದಾಯ ತೆರಿಗೆ",
            "ಪ್ರಮಾಣಪತ್ರ ಸಂಖ್ಯೆ",
            "ತೆರಿಗೆ ಪಾವತಿ",
            "ಆದಾಯ ವಿವರಗಳು",
        ],
        "mr": [
            "फॉर्म 16",
            "टीडीएस प्रमाणपत्र",
            "कर कपात",
            "स्रोतातून कपात",
            "टॅन क्रमांक",
            "चलन क्रमांक",
            "कर निर्धारण वर्ष",
            "आर्थिक वर्ष",
            "त्रैमासिक",
            "नियोक्ता तपशील",
            "कर्मचारी तपशील",
            "कपातकर्ता तपशील",
            "कपात केलेल्याचे तपशील",
            "भाग अ",
            "भाग ब",
            "पगार तपशील",
            "आयकर",
            "प्रमाणपत्र क्रमांक",
            "कर भरणा",
            "उत्पन्न तपशील",
        ],
        "gu": [
            "ફોર્મ 16",
            "ટીડીએસ પ્રમાણપત્ર",
            "કર કપાત",
            "સ્ત્રોત કપાત",
            "ટેન નંબર",
            "ચલણ નંબર",
            "કર આકારણી વર્ષ",
            "નાણાકીય વર્ષ",
            "ત્રિમાસિક",
            "નિયોક્તા વિગતો",
            "કર્મચારી વિગતો",
            "કપાતકર્તા વિગતો",
            "કપાત થનાર વિગતો",
            "ભાગ એ",
            "ભાગ બી",
            "પગાર વિગતો",
            "આવકવેરો",
            "પ્રમાણપત્ર નંબર",
            "કર ચુકવણી",
            "આવક વિગતો",
        ],
    }

    VALID_PREFIXES = {
        "F16",
        "TDS",
        "TAN",
        "AY",
        "FY",
        "Q1",
        "Q2",
        "Q3",
        "Q4",
    }

    VALID_CITIES = {
        "MUM",  # Mumbai
        "DEL",  # Delhi
        "BLR",  # Bangalore
        "CHN",  # Chennai
        "KOL",  # Kolkata
        "HYD",  # Hyderabad
        "PUN",  # Pune
        "AHM",  # Ahmedabad
        "BPL",  # Bhopal
        "LKN",  # Lucknow
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_FORM16_TDS",
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
        return self.__check_form16_reference(sanitized_value)

    def __check_form16_reference(self, sanitized_value: str) -> bool:
        """
        Validate Form 16 and TDS reference formats.
        
        Checks:
        1. Length validation
        2. Prefix validation
        3. City code validation
        4. Quarter validation
        5. Year format validation
        6. TAN format validation
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 20:
            return False

        # TAN number check
        if len(sanitized_value) == 10:
            if (sanitized_value[:4].isalpha() and 
                sanitized_value[4:9].isdigit() and 
                sanitized_value[9].isalpha()):
                return True

        # Check for valid prefixes
        for prefix in self.VALID_PREFIXES:
            if sanitized_value.upper().startswith(prefix):
                remaining = sanitized_value[len(prefix):]
                return any(char.isdigit() for char in remaining)

        # Check for city codes in TDS certificate numbers
        for city in self.VALID_CITIES:
            if sanitized_value.upper().startswith(city):
                remaining = sanitized_value[len(city):]
                return remaining.isalnum()

        # Check for assessment year format
        if sanitized_value.startswith(("AY", "FY")):
            year_part = sanitized_value[2:]
            if len(year_part) == 9 and year_part[4] == "-":
                return (year_part[:4].isdigit() and 
                       year_part[5:].isdigit() and 
                       2000 <= int(year_part[:4]) <= 2100)

        # Check for quarter reference
        if sanitized_value.startswith("Q"):
            if (sanitized_value[1] in "1234" and 
                sanitized_value[2:].isdigit()):
                return True

        # General alphanumeric validation
        return (
            sanitized_value.isalnum() and 
            any(char.isdigit() for char in sanitized_value) and 
            any(char.isalpha() for char in sanitized_value)
        )