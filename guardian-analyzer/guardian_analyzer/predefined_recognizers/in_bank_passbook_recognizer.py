from typing import List, Optional, Tuple

from guardian_analyzer import Pattern, PatternRecognizer
from guardian_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InBankPassbookRecognizer(PatternRecognizer):
    """
    Recognizes Indian Bank Passbook identifiers in multiple languages.
    
    Includes patterns for:
    - Passbook numbers
    - Account numbers
    - IFSC codes
    - Customer IDs
    - Branch codes
    - Passbook serial numbers
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Passbook Number (Basic)",
            r"\b(PB|PSB)[A-Z0-9]{8,12}\b",
            0.4,
        ),
        Pattern(
            "Passbook Number (With Bank)",
            r"\b[A-Z]{2,4}PB\d{8,12}\b",
            0.6,
        ),
        Pattern(
            "Account Number (Basic)",
            r"\b\d{9,18}\b",
            0.3,
        ),
        Pattern(
            "Account Number (Formatted)",
            r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{0,6}\b",
            0.5,
        ),
        Pattern(
            "IFSC Code",
            r"\b[A-Z]{4}0[A-Z0-9]{6}\b",
            0.7,
        ),
        Pattern(
            "Customer ID",
            r"\b(CID|CUSTID)[0-9]{6,12}\b",
            0.6,
        ),
        Pattern(
            "Branch Code",
            r"\b[A-Z]{2,4}\/[A-Z0-9]{4,6}\b",
            0.55,
        ),
        Pattern(
            "Passbook Serial",
            r"\b[A-Z]{2,4}-PB-\d{6,10}\b",
            0.65,
        ),
        Pattern(
            "Account Type Code",
            r"\b(SB|CA|RD|FD|TD)\d{6,12}\b",
            0.45,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "passbook",
            "bank passbook",
            "account passbook",
            "savings passbook",
            "passbook number",
            "account number",
            "customer id",
            "ifsc code",
            "branch code",
            "serial number",
            "account type",
            "bank details",
            "account details",
            "branch details",
            "savings account",
            "current account",
            "fixed deposit",
            "recurring deposit",
            "bank branch",
            "issue date",
        ],
        "hi": [
            "पासबुक",
            "बैंक पासबुक",
            "खाता पासबुक",
            "बचत पासबुक",
            "पासबुक नंबर",
            "खाता संख्या",
            "ग्राहक आईडी",
            "आईएफएससी कोड",
            "शाखा कोड",
            "क्रम संख्या",
            "खाता प्रकार",
            "बैंक विवरण",
            "खाता विवरण",
            "शाखा विवरण",
            "बचत खाता",
            "चालू खाता",
            "सावधि जमा",
            "आवर्ती जमा",
            "बैंक शाखा",
            "जारी तिथि",
        ],
        "ta": [
            "பாஸ்புக்",
            "வங்கி பாஸ்புக்",
            "கணக்கு பாஸ்புக்",
            "சேமிப்பு பாஸ்புக்",
            "பாஸ்புக் எண்",
            "கணக்கு எண்",
            "வாடிக்கையாளர் ஐடி",
            "ஐஎஃப்எஸ்சி குறியீடு",
            "கிளை குறியீடு",
            "வரிசை எண்",
            "கணக்கு வகை",
            "வங்கி விவரங்கள்",
            "கணக்கு விவரங்கள்",
            "கிளை விவரங்கள்",
            "சேமிப்பு கணக்கு",
            "நடப்பு கணக்கு",
            "நிலையான வைப்பு",
            "தொடர் வைப்பு",
            "வங்கி கிளை",
            "வழங்கல் தேதி",
        ],
        "te": [
            "పాస్ బుక్",
            "బ్యాంకు పాస్ బుక్",
            "ఖాతా పాస్ బుక్",
            "సేవింగ్స్ పాస్ బుక్",
            "పాస్ బుక్ నంబర్",
            "ఖాతా సంఖ్య",
            "కస్టమర్ ఐడి",
            "ఐఎఫ్ఎస్సి కోడ్",
            "బ్రాంచ్ కోడ్",
            "క్రమ సంఖ్య",
            "ఖాతా రకం",
            "బ్యాంకు వివరాలు",
            "ఖాతా వివరాలు",
            "బ్రాంచ్ వివరాలు",
            "సేవింగ్స్ ఖాతా",
            "కరెంట్ ఖాతా",
            "ఫిక్స్డ్ డిపాజిట్",
            "రికరింగ్ డిపాజిట్",
            "బ్యాంకు బ్రాంచ్",
            "జారీ తేదీ",
        ],
        "kn": [
            "ಪಾಸ್‌ಬುಕ್",
            "ಬ್ಯಾಂಕ್ ಪಾಸ್‌ಬುಕ್",
            "ಖಾತೆ ಪಾಸ್‌ಬುಕ್",
            "ಉಳಿತಾಯ ಪಾಸ್‌ಬುಕ್",
            "ಪಾಸ್‌ಬುಕ್ ಸಂಖ್ಯೆ",
            "ಖಾತೆ ಸಂಖ್ಯೆ",
            "ಗ್ರಾಹಕ ಐಡಿ",
            "ಐಎಫ್‌ಎಸ್‌ಸಿ ಕೋಡ್",
            "ಶಾಖೆ ಕೋಡ್",
            "ಕ್ರಮ ಸಂಖ್ಯೆ",
            "ಖಾತೆ ಪ್ರಕಾರ",
            "ಬ್ಯಾಂಕ್ ವಿವರಗಳು",
            "ಖಾತೆ ವಿವರಗಳು",
            "ಶಾಖೆ ವಿವರಗಳು",
            "ಉಳಿತಾಯ ಖಾತೆ",
            "ಚಾಲ್ತಿ ಖಾತೆ",
            "ಸ್ಥಿರ ಠೇವಣಿ",
            "ಆವರ್ತಕ ಠೇವಣಿ",
            "ಬ್ಯಾಂಕ್ ಶಾಖೆ",
            "ವಿತರಣೆ ದಿನಾಂಕ",
        ],
        "mr": [
            "पासबुक",
            "बँक पासबुक",
            "खाते पासबुक",
            "बचत पासबुक",
            "पासबुक क्रमांक",
            "खाते क्रमांक",
            "ग्राहक आयडी",
            "आयएफएससी कोड",
            "शाखा कोड",
            "अनुक्रमांक",
            "खाते प्रकार",
            "बँक तपशील",
            "खाते तपशील",
            "शाखा तपशील",
            "बचत खाते",
            "चालू खाते",
            "मुदत ठेव",
            "आवर्ती ठेव",
            "बँक शाखा",
            "जारी दिनांक",
        ],
        "gu": [
            "પાસબુક",
            "બેંક પાસબુક",
            "ખાતા પાસબુક",
            "બચત પાસબુક",
            "પાસબુક નંબર",
            "ખાતા નંબર",
            "ગ્રાહક આઈડી",
            "આઈએફએસસી કોડ",
            "શાખા કોડ",
            "ક્રમ નંબર",
            "ખાતા પ્રકાર",
            "બેંક વિગતો",
            "ખાતા વિગતો",
            "શાખા વિગતો",
            "બચત ખાતું",
            "ચાલુ ખાતું",
            "સ્થાયી થાપણ",
            "આવર્તક થાપણ",
            "બેંક શાખા",
            "જારી તારીખ",
        ],
    }

    VALID_PREFIXES = {
        "PB",
        "PSB",
        "CID",
        "CUSTID",
        "SB",  # Savings Bank
        "CA",  # Current Account
        "RD",  # Recurring Deposit
        "FD",  # Fixed Deposit
        "TD",  # Term Deposit
    }

    VALID_BANKS = {
        "SBI",  # State Bank of India
        "HDFC",
        "ICICI",
        "PNB",  # Punjab National Bank
        "BOB",  # Bank of Baroda
        "AXIS",
        "UBI",  # Union Bank of India
        "BOI",  # Bank of India
        "CBI",  # Central Bank of India
        "IOB",  # Indian Overseas Bank
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_BANK_PASSBOOK",
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
        return self.__check_passbook_reference(sanitized_value)

    def __check_passbook_reference(self, sanitized_value: str) -> bool:
        """
        Validate the bank passbook reference format.
        
        Checks:
        1. Length validation
        2. Prefix validation
        3. Bank code validation
        4. Account type validation
        5. Character set validation
        6. IFSC code validation
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 20:
            return False

        # IFSC code check
        if len(sanitized_value) == 11 and sanitized_value[4] == "0":
            bank_code = sanitized_value[:4]
            return (
                bank_code.isalpha()
                and bank_code.isupper()
                and sanitized_value[5:].isalnum()
            )

        # Check for valid prefixes
        for prefix in self.VALID_PREFIXES:
            if sanitized_value.upper().startswith(prefix):
                remaining = sanitized_value[len(prefix):]
                return any(char.isdigit() for char in remaining)

        # Check for bank codes
        for bank in self.VALID_BANKS:
            if sanitized_value.upper().startswith(bank):
                remaining = sanitized_value[len(bank):]
                return any(char.isdigit() for char in remaining)

        # Account number validation
        if sanitized_value.isdigit():
            return 9 <= len(sanitized_value) <= 18

        # Check for passbook-specific format (e.g., SBI-PB-123456)
        if "-PB-" in sanitized_value:
            parts = sanitized_value.split("-")
            return (
                len(parts) == 3
                and parts[0] in self.VALID_BANKS
                and parts[1] == "PB"
                and parts[2].isdigit()
            )

        # General alphanumeric validation
        return (
            sanitized_value.isalnum()
            and any(char.isdigit() for char in sanitized_value)
            and any(char.isalpha() for char in sanitized_value)
        ) 