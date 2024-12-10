from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InEducationMarksheetRecognizer(PatternRecognizer):
    """
    Recognizes Indian Educational Marksheet identifiers in multiple languages.
    
    Includes patterns for:
    - 10th marksheet numbers
    - 12th marksheet numbers
    - Diploma certificate numbers
    - Graduation marksheet numbers
    - Roll numbers
    - Registration numbers
    - Certificate numbers
    - Seat numbers
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "10th Marksheet",
            r"\b(SSC|X)[A-Z]{2}[0-9]{8,12}\b",  # Format: SSCMH12345678
            0.7,
        ),
        Pattern(
            "12th Marksheet",
            r"\b(HSC|XII)[A-Z]{2}[0-9]{8,12}\b",  # Format: HSCMH12345678
            0.7,
        ),
        Pattern(
            "Diploma Certificate",
            r"\b(DIP|DIPL)[A-Z]{2,4}[0-9]{8,12}\b",  # Format: DIPMECH12345678
            0.65,
        ),
        Pattern(
            "Graduation Marksheet",
            r"\b[A-Z]{2,4}(BE|BTECH|BSC|BCOM|BA)[0-9]{8,12}\b",  # Format: MUBE12345678
            0.6,
        ),
        Pattern(
            "Roll Number",
            r"\b[0-9]{2}[A-Z]{2,4}[0-9]{6,8}\b",  # Format: 19CS123456
            0.55,
        ),
        Pattern(
            "Registration Number",
            r"\b[A-Z]{1,3}\/[0-9]{2}\/[0-9]{4,8}\b",  # Format: CS/19/123456
            0.5,
        ),
        Pattern(
            "Certificate Number",
            r"\b[A-Z]{2,4}\/CERT\/[0-9]{6,10}\b",  # Format: MH/CERT/123456
            0.6,
        ),
        Pattern(
            "Seat Number",
            r"\b[A-Z]{1,3}[0-9]{2}[A-Z][0-9]{4,6}\b",  # Format: CS19A1234
            0.45,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "marksheet",
            "mark sheet",
            "statement of marks",
            "grade sheet",
            "result card",
            "academic record",
            "roll number",
            "registration number",
            "certificate number",
            "seat number",
            "examination",
            "board",
            "university",
            "passing year",
            "division",
            "percentage",
            "grade",
            "subjects",
            "total marks",
            "obtained marks",
        ],
        "hi": [
            "मार्कशीट",
            "अंक तालिका",
            "अंक पत्र",
            "ग्रेड शीट",
            "परिणाम कार्ड",
            "शैक्षणिक रिकॉर्ड",
            "रोल नंबर",
            "पंजीकरण संख्या",
            "प्रमाणपत्र संख्या",
            "सीट नंबर",
            "परीक्षा",
            "बोर्ड",
            "विश्वविद्यालय",
            "उत्तीर्ण वर्ष",
            "श्रेणी",
            "प्रतिशत",
            "ग्रेड",
            "विषय",
            "कुल अंक",
            "प्राप्त अंक",
        ],
        # Add similar entries for ta, te, kn, mr, gu
    }

    VALID_PREFIXES = {
        "SSC",    # Secondary School Certificate
        "HSC",    # Higher Secondary Certificate
        "DIP",    # Diploma
        "DIPL",   # Diploma
        "BE",     # Bachelor of Engineering
        "BTECH",  # Bachelor of Technology
        "BSC",    # Bachelor of Science
        "BCOM",   # Bachelor of Commerce
        "BA",     # Bachelor of Arts
    }

    VALID_BOARDS = {
        "CBSE",  # Central Board
        "ICSE",  # Indian Certificate
        "IB",    # International Baccalaureate
        "MH",    # Maharashtra
        "UP",    # Uttar Pradesh
        "KA",    # Karnataka
        "TN",    # Tamil Nadu
        "AP",    # Andhra Pradesh
        "WB",    # West Bengal
        "GJ",    # Gujarat
    }

    VALID_STREAMS = {
        "SCI",   # Science
        "COM",   # Commerce
        "ARTS",  # Arts
        "MECH",  # Mechanical
        "CS",    # Computer Science
        "IT",    # Information Technology
        "EE",    # Electrical
        "EC",    # Electronics
        "CIVIL", # Civil
        "CHEM",  # Chemical
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_EDUCATION_MARKSHEET",
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
        return self.__check_marksheet_reference(sanitized_value)

    def __check_marksheet_reference(self, sanitized_value: str) -> bool:
        """
        Validate educational marksheet reference formats.
        
        Checks:
        1. Length validation
        2. Prefix validation
        3. Board code validation
        4. Stream code validation
        5. Year format validation
        6. Number format validation
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 20:
            return False

        # Check for valid education level prefixes
        for prefix in self.VALID_PREFIXES:
            if sanitized_value.upper().startswith(prefix):
                remaining = sanitized_value[len(prefix):]
                # Check if remaining part contains a valid board code
                for board in self.VALID_BOARDS:
                    if remaining.startswith(board):
                        number_part = remaining[len(board):]
                        return number_part.isdigit() and len(number_part) >= 6

        # Check for roll number format with stream codes
        for stream in self.VALID_STREAMS:
            if stream in sanitized_value.upper():
                parts = sanitized_value.split(stream)
                if len(parts) == 2:
                    return (
                        (parts[0].isdigit() or parts[0].isalpha()) and
                        parts[1].isdigit()
                    )

        # Check for registration number format
        if "/" in sanitized_value:
            parts = sanitized_value.split("/")
            if len(parts) == 3:
                return (
                    (parts[0] in self.VALID_STREAMS or parts[0] in self.VALID_BOARDS) and
                    parts[1].isdigit() and len(parts[1]) == 2 and
                    parts[2].isdigit() and len(parts[2]) >= 4
                )

        # Check for year-based format (YY in middle)
        if len(sanitized_value) >= 8:
            middle_part = sanitized_value[2:4]
            if middle_part.isdigit() and 0 <= int(middle_part) <= 99:
                return sanitized_value[4:].isalnum()

        # General alphanumeric validation
        return (
            sanitized_value.isalnum() and
            any(char.isdigit() for char in sanitized_value) and
            any(char.isalpha() for char in sanitized_value)
        )
 