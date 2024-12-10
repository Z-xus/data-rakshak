from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InBankStatementRecognizer(PatternRecognizer):
    """
    Recognizes Indian Bank Statement identifiers in multiple languages.
    
    Includes patterns for:
    - Statement reference numbers
    - Account numbers
    - IFSC codes
    - Transaction reference numbers
    - Statement sequence numbers
    - Bank statement IDs
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Statement Reference (Basic)",
            r"\b(STM|STMT|BS)[A-Z0-9]{8,12}\b",
            0.4,
        ),
        Pattern(
            "Statement Reference (Date)",
            r"\b(STM|STMT)\d{6,8}[A-Z0-9]{4,8}\b",
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
            "Transaction Reference",
            r"\b(TXN|REF|TR)[A-Z0-9]{10,15}\b",
            0.5,
        ),
        Pattern(
            "Statement Sequence",
            r"\b\d{4}\/[A-Z]{2,4}\/\d{6,8}\b",
            0.55,
        ),
        Pattern(
            "Bank Statement ID",
            r"\b[A-Z]{2,4}-\d{2}(0[1-9]|1[0-2])-\d{4,8}\b",
            0.65,
        ),
        Pattern(
            "Customer ID",
            r"\b(CID|CUSTID)[0-9]{6,12}\b",
            0.6,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "bank statement",
            "statement number",
            "reference number",
            "transaction history",
            "account statement",
            "statement period",
            "statement date",
            "bank record",
            "account number",
            "customer id",
            "ifsc code",
            "branch code",
            "transaction id",
            "balance",
            "credit",
            "debit",
            "passbook",
            "bank details",
            "account details",
            "statement ref",
        ],
        "hi": [
            "बैंक स्टेटमेंट",
            "खाता विवरण",
            "लेनदेन इतिहास",
            "संदर्भ संख्या",
            "विवरण संख्या",
            "खाता संख्या",
            "ग्राहक आईडी",
            "शाखा कोड",
            "लेन-देन आईडी",
            "शेष राशि",
            "जमा",
            "नामे",
            "पासबुक",
            "बैंक विवरण",
            "खाता जानकारी",
            "विवरण अवधि",
            "विवरण तिथि",
            "आईएफएससी कोड",
            "बैंक रिकॉर्ड",
            "स्टेटमेंट संदर्भ",
        ],
        "ta": [
            "வங்கி அறிக்கை",
            "கணக்கு அறிக்கை",
            "பரிவர்த்தனை வரலாறு",
            "குறிப்பு எண்",
            "கணக்கு எண்",
            "வாடிக்கையாளர் ஐடி",
            "கிளை குறியீடு",
            "பரிவர்த்தனை ஐடி",
            "இருப்பு",
            "வரவு",
            "பற்று",
            "பாஸ்புக்",
            "வங்கி விவரங்கள்",
            "கணக்கு விவரங்கள்",
            "அறிக்கை காலம்",
            "அறிக்கை தேதி",
            "ஐஎஃப்எஸ்சி குறியீடு",
            "வங்கி பதிவு",
            "அறிக்கை குறிப்பு",
            "பரிவர்த்தனை எண்",
        ],
        "te": [
            "బ్యాంకు స్టేట్మెంట్",
            "ఖాతా వివరణ",
            "లావాదేవీల చరిత్ర",
            "సూచన సంఖ్య",
            "ఖాతా సంఖ్య",
            "కస్టమర్ ఐడి",
            "బ్రాంచ్ కోడ్",
            "లావాదేవీ ఐడి",
            "నిల్వ",
            "జమ",
            "డెబిట్",
            "పాస్ బుక్",
            "బ్యాంకు వివరాలు",
            "ఖాతా వివరాలు",
            "స్టేట్మెంట్ కాలం",
            "స్టేట్మెంట్ తేదీ",
            "ఐఎఫ్ఎస్సి కోడ్",
            "బ్యాంకు రికార్డు",
            "స్టేట్మెంట్ రెఫరెన్స్",
            "లావాదేవీ సంఖ్య",
        ],
        "kn": [
            "ಬ್ಯಾಂಕ್ ಸ್ಟೇಟ್ಮೆಂಟ್",
            "ಖಾತೆ ವಿವರಣೆ",
            "ವಹಿವಾಟು ಇತಿಹಾಸ",
            "ಉಲ್ಲೇಖ ಸಂಖ್ಯೆ",
            "ಖಾತೆ ಸಂಖ್ಯೆ",
            "ಗ್ರಾಹಕ ಐಡಿ",
            "ಶಾಖೆ ಕೋಡ್",
            "ವಹಿವಾಟು ಐಡಿ",
            "ಬ್ಯಾಲೆನ್ಸ್",
            "ಕ್ರೆಡಿಟ್",
            "ಡೆಬಿಟ್",
            "ಪಾಸ್‌ಬುಕ್",
            "ಬ್ಯಾಂಕ್ ವಿವರಗಳು",
            "ಖಾತೆ ವಿವರಗಳು",
            "ಸ್ಟೇಟ್ಮೆಂಟ್ ಅವಧಿ",
            "ಸ್ಟೇಟ್ಮೆಂಟ್ ದಿನಾಂಕ",
            "ಐಎಫ್‌ಎಸ್‌ಸಿ ಕೋಡ್",
            "ಬ್ಯಾಂಕ್ ದಾಖಲೆ",
            "ಸ್ಟೇಟ್ಮೆಂಟ್ ಉಲ್ಲೇಖ",
            "ವಹಿವಾಟು ಸಂಖ್ಯೆ",
        ],
        "mr": [
            "बँक स्टेटमेंट",
            "खाते विवरण",
            "व्यवहार इतिहास",
            "संदर्भ क्रमांक",
            "खाते क्रमांक",
            "ग्राहक आयडी",
            "शाखा कोड",
            "व्यवहार आयडी",
            "शिल्लक",
            "जमा",
            "नावे",
            "पासबुक",
            "बँक तपशील",
            "खाते तपशील",
            "स्टेटमेंट कालावधी",
            "स्टेटमेंट दिनांक",
            "आयएफएससी कोड",
            "बँक रेकॉर्ड",
            "स्टेटमेंट संदर्भ",
            "व्यवहार क्रमांक",
        ],
        "gu": [
            "બેંક સ્ટેટમેન્ટ",
            "ખાતા વિવરણ",
            "વ્યવહાર ઇતિહાસ",
            "સંદર્ભ નંબર",
            "ખાતા નંબર",
            "ગ્રાહક આઈડી",
            "શાખા કોડ",
            "વ્યવહાર આઈડી",
            "બેલેન્સ",
            "જમા",
            "ઉધાર",
            "પાસબુક",
            "બેંક વિગતો",
            "ખાતા વિગતો",
            "સ્ટેટમેન્ટ સમયગાળો",
            "સ્ટેટમેન્ટ તારીખ",
            "આઈએફએસસી કોડ",
            "બેંક રેકોર્ડ",
            "સ્ટેટમેન્ટ રેફરન્સ",
            "વ્યવહાર નંબર",
        ],
    }

    VALID_PREFIXES = {
        "STM",
        "STMT",
        "BS",
        "TXN",
        "REF",
        "BNK",
        "CID",
        "CUSTID",
        "TR",
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
        supported_entity: str = "IN_BANK_STATEMENT",
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
        return self.__check_statement_reference(sanitized_value)

    def __check_statement_reference(self, sanitized_value: str) -> bool:
        """
        Validate the bank statement reference format.
        
        Checks:
        1. Length validation
        2. Prefix validation
        3. Bank code validation
        4. Date component validation (if present)
        5. Character set validation
        6. IFSC code validation
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 25:
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

        # Check for date-based format (YYYYMM or YYYY-MM)
        if len(sanitized_value) >= 12:
            potential_year = sanitized_value[:4]
            potential_month = sanitized_value[4:6]
            if (
                potential_year.isdigit()
                and potential_month.isdigit()
                and 2000 <= int(potential_year) <= 2100
                and 1 <= int(potential_month) <= 12
            ):
                return True

        # Account number validation
        if sanitized_value.isdigit():
            return 9 <= len(sanitized_value) <= 18

        # General alphanumeric validation
        return (
            sanitized_value.isalnum()
            and any(char.isdigit() for char in sanitized_value)
            and any(char.isalpha() for char in sanitized_value)
        )