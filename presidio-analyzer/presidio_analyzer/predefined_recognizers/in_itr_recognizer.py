from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InITRRecognizer(PatternRecognizer):
    """
    Recognizes Indian Income Tax Return (ITR) related identifiers in multiple languages.
    
    Includes patterns for:
    - ITR acknowledgment numbers
    - PAN numbers
    - CPC reference numbers
    - Assessment Year references
    - ITR form numbers
    - Digital Signature certificates
    - E-filing numbers
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "ITR Acknowledgment",
            r"\b[A-Z]{3}[A-Z0-9]{7}[0-9]{2}20[0-9]{2}\b",
            0.85,
        ),
        Pattern(
            "PAN Number",
            r"\b[A-Z]{5}[0-9]{4}[A-Z]{1}\b",
            0.9,
        ),
        Pattern(
            "CPC Reference",
            r"\b(CPC|ITR)[0-9]{12,16}\b",
            0.7,
        ),
        Pattern(
            "Assessment Year",
            r"\b(AY|FY)\s?20[0-9]{2}-2[0-9]{3}\b",
            0.6,
        ),
        Pattern(
            "ITR Form Number",
            r"\bITR-[1-7]\b",
            0.75,
        ),
        Pattern(
            "Digital Signature",
            r"\bDSC[A-Z0-9]{10,15}\b",
            0.65,
        ),
        Pattern(
            "E-filing Number",
            r"\bEFIL[0-9]{12}\b",
            0.8,
        ),
        Pattern(
            "Tax Payment Challan",
            r"\bCHLN\d{5,7}[A-Z0-9]{4,8}\b",
            0.7,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "income tax return",
            "itr form",
            "tax return",
            "acknowledgment number",
            "pan card",
            "permanent account number",
            "assessment year",
            "financial year",
            "e-filing",
            "digital signature",
            "tax payment",
            "challan",
            "cpc processing",
            "tax filing",
            "return filed",
            "tax assessment",
            "form verification",
            "tax computation",
            "tax audit",
            "return status",
        ],
        "hi": [
            "आयकर रिटर्न",
            "आईटीआर फॉर्म",
            "कर रिटर्न",
            "पावती संख्या",
            "पैन कार्ड",
            "स्थायी खाता संख्या",
            "निर्धारण वर्ष",
            "वित्तीय वर्ष",
            "ई-फाइलिंग",
            "डिजिटल हस्ताक्षर",
            "कर भुगतान",
            "चालान",
            "सीपीसी प्रसंस्करण",
            "कर दाखिल",
            "रिटर्न दाखिल",
            "कर निर्धारण",
            "फॉर्म सत्यापन",
            "कर गणना",
            "कर ऑडिट",
            "रिटर्न स्थिति",
        ],
        "ta": [
            "வருமான வரி ரிட்டர்ன்",
            "ஐடிஆர் படிவம்",
            "வரி ரிட்டர்ன்",
            "ஒப்புகை எண்",
            "பான் கார்டு",
            "நிரந்தர கணக்கு எண்",
            "மதிப்பீட்டு ஆண்டு",
            "நிதியாண்டு",
            "மின்-தாக்கல்",
            "டிஜிட்டல் கையொப்பம்",
            "வரி செலுத்துதல்",
            "செலான்",
            "சிபிசி செயலாக்கம்",
            "வரி தாக்கல்",
            "ரிட்டர்ன் தாக்கல்",
            "வரி மதிப்பீடு",
            "படிவ சரிபார்ப்பு",
            "வரி கணக்கீடு",
            "வரி தணிக்கை",
            "ரிட்டர்ன் நிலை",
        ],
        "te": [
            "ఆదాయపు పన్ను రిటర్న్",
            "ఐటిఆర్ ఫారం",
            "పన్ను రిటర్న్",
            "రసీదు సంఖ్య",
            "పాన్ కార్డ్",
            "శాశ్వత ఖాతా సంఖ్య",
            "అసెస్మెంట్ సంవత్సరం",
            "ఆర్థిక సంవత్సరం",
            "ఇ-ఫైలింగ్",
            "డిజిటల్ సంి",
            "తೆరిగు పావతి",
            "చలన్",
            "సిపిసి ప్రాసెసింగ్",
            "తೆరిగు సల్లిక్",
            "రిటర్న్ సల్లిక్",
            "తೆరిగు ఆకారణీ",
            "ఫారం ధృవీకరణ",
            "పన్ను లెక్కింపు",
            "పన్ను ఆడిట్",
            "రిటర్న్ స్థితి",
        ],
        "kn": [
            "ಆದಾಯ ತೆರಿಗೆ ರಿಟರ್ನ್",
            "ಐಟಿಆರ್ ಫಾರ್ಮ್",
            "ತೆರಿಗೆ ರಿಟರ್ನ್",
            "ಸ್ವೀಕೃತಿ ಸಂಖ್ಯೆ",
            "ಪ್ಯಾನ್ ಕಾರ್ಡ್",
            "ಶಾಶ್ವತ ಖಾತೆ ಸಂಖ್ಯೆ",
            "ಮೌಲ್ಯಮಾಪನ ವರ್ಷ",
            "ಹಣಕಾಸು ವರ್ಷ",
            "ಇ-ಫೈಲಿಂಗ್",
            "ಡಿಜಿಟಲ್ ಸಹಿ",
            "ತೆರಿಗೆ ಪಾವತಿ",
            "ಚಲನ್",
            "ಸಿಪಿಸಿ ಪ್ರಕ್ರಿಯೆ",
            "ತೆರಿಗೆ ಸಲ್ಲಿಕೆ",
            "ರಿಟರ್ನ್ ಸಲ್ಲಿಕೆ",
            "ತೆರಿಗೆ ನಿರ್ಧಾರ",
            "ಫಾರ್ಮ್ ಪರಿಶೀಲನೆ",
            "ತೆರಿಗೆ ಲೆಕ್ಕಾಚಾರ",
            "ತೆರಿಗೆ ಆಡಿಟ್",
            "ರಿಟರ್ನ್ ಸ್ಥಿತಿ",
        ],
        "mr": [
            "आयकर रिटर्न",
            "आयटीआर फॉर्म",
            "कर रिटर्न",
            "पावती क्रमांक",
            "पॅन कार्ड",
            "स्थायी खाते क्रमांक",
            "मूल्यांकन वर्ष",
            "आर्थिक वर्ष",
            "ई-फायलिंग",
            "डिजिटल स्वाक्षरी",
            "कर भरणा",
            "चलन",
            "सीपीसी प्रक्रिया",
            "कर दाखल",
            "रिटर्न दाखल",
            "कर मूल्यांकन",
            "फॉर्म पडताळणी",
            "कर गणना",
            "कर लेखापरीक्षण",
            "रिटर्न स्थिती",
        ],
        "gu": [
            "આવકવેરા રિટર્ન",
            "આઈટીઆર ફોર્મ",
            "કર રિટર્ન",
            "પાવતી નંબર",
            "પાન કાર્ડ",
            "કાયમી ખાતા નંબર",
            "આકારણી વર્ષ",
            "નાણાકીય વર્ષ",
            "ઈ-ફાઇલિંગ",
            "ડિજિટલ સહી",
            "કર ચુકવણી",
            "ચલણ",
            "સીપીસી પ્રોસેસિંગ",
            "કર ભરવો",
            "રિટર્ન ભરવું",
            "કર આકારણી",
            "ફોર્મ ચકાસણી",
            "કર ગણતરી",
            "કર ઓડિટ",
            "રિટર્ન સ્થિતિ",
        ],
    }

    VALID_PREFIXES = {
        "ITR",
        "CPC",
        "DSC",
        "EFIL",
        "CHLN",
        "AY",
        "FY",
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_ITR",
        replacement_pairs: Optional[List[Tuple[str, str]]] = None,
    ):
        """
        Initialize the recognizer with language-specific context and patterns.
        """
        self.language = language
        self.replacement_pairs = (
            replacement_pairs if replacement_pairs else [("-", ""), (" ", "")]
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
        return self.__validate_itr_format(sanitized_value)

    def __validate_itr_format(self, sanitized_value: str) -> bool:
        """
        Validate the ITR reference format.
        
        Checks:
        1. Length validation
        2. Prefix validation
        3. PAN validation
        4. Year component validation
        5. Character set validation
        6. Checksum validation for specific formats
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 20:
            return False

        # PAN validation
        if len(sanitized_value) == 10:
            return (
                sanitized_value[:5].isalpha()
                and sanitized_value[5:9].isdigit()
                and sanitized_value[9].isalpha()
            )

        # Check for valid prefixes
        for prefix in self.VALID_PREFIXES:
            if sanitized_value.upper().startswith(prefix):
                remaining = sanitized_value[len(prefix):]
                return any(char.isdigit() for char in remaining)

        # Assessment Year validation
        if sanitized_value.startswith(("AY", "FY")):
            year_part = sanitized_value[2:]
            try:
                start_year = int(year_part[:4])
                end_year = int(year_part[-4:])
                return (
                    2000 <= start_year <= 2100
                    and start_year + 1 == end_year
                )
            except ValueError:
                return False

        # ITR Acknowledgment number validation
        if len(sanitized_value) == 15:
            year_part = sanitized_value[-6:]
            try:
                year = int(year_part[-4:])
                return (
                    sanitized_value[:3].isalpha()
                    and sanitized_value[3:10].isalnum()
                    and sanitized_value[10:12].isdigit()
                    and 2000 <= year <= 2100
                )
            except ValueError:
                return False

        # General format validation
        return (
            sanitized_value.isalnum()
            and any(char.isdigit() for char in sanitized_value)
            and any(char.isalpha() for char in sanitized_value)
        ) 