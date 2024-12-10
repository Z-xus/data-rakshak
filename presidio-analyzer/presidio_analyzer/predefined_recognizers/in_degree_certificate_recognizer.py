from typing import List, Optional, Tuple

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_analyzer.analyzer_utils import PresidioAnalyzerUtils as Utils


class InDegreeCertificateRecognizer(PatternRecognizer):
    """
    Recognizes Indian Degree Certificate identifiers in multiple languages.
    
    Includes patterns for:
    - Bachelor's degree certificates
    - Master's degree certificates
    - Diploma certificates
    - Professional degree certificates
    - Doctorate certificates
    - Provisional certificates
    - Convocation certificates
    """

    DEFAULT_PATTERNS = [
        Pattern(
            "Bachelor Degree",
            r"\b(BE|BTECH|BSC|BCOM|BA|BBA|BCA)[A-Z]{2}[0-9]{8,12}\b",  # Format: BTECHMU12345678
            0.7,
        ),
        Pattern(
            "Master Degree",
            r"\b(ME|MTECH|MSC|MCOM|MA|MBA|MCA)[A-Z]{2}[0-9]{8,12}\b",  # Format: MTECHMU12345678
            0.7,
        ),
        Pattern(
            "Professional Degree",
            r"\b(MBBS|BDS|BAMS|BHMS|LLB)[A-Z]{2}[0-9]{8,12}\b",  # Format: MBBSMU12345678
            0.75,
        ),
        Pattern(
            "Doctorate",
            r"\b(PHD|DPHIL)[A-Z]{2,4}[0-9]{8,12}\b",  # Format: PHDMU12345678
            0.8,
        ),
        Pattern(
            "Certificate Number",
            r"\b[A-Z]{2,4}\/DEG\/[0-9]{2}\/[0-9]{4,8}\b",  # Format: MU/DEG/23/12345
            0.65,
        ),
        Pattern(
            "Provisional Certificate",
            r"\b[A-Z]{2,4}\/PROV\/[0-9]{2}\/[0-9]{4,8}\b",  # Format: MU/PROV/23/12345
            0.6,
        ),
        Pattern(
            "Convocation Number",
            r"\b[A-Z]{2,4}\/CONV\/[0-9]{2}\/[0-9]{4,8}\b",  # Format: MU/CONV/23/12345
            0.6,
        ),
        Pattern(
            "Registration Number",
            r"\b[A-Z]{2,4}[0-9]{2}[A-Z]{2,4}[0-9]{4,6}\b",  # Format: MU19CS1234
            0.55,
        ),
    ]

    CONTEXT_MAP = {
        "en": [
            "degree certificate",
            "graduation certificate",
            "bachelor degree",
            "master degree",
            "doctorate degree",
            "provisional certificate",
            "convocation certificate",
            "university name",
            "registration number",
            "certificate number",
            "degree awarded",
            "specialization",
            "graduation date",
            "convocation date",
            "academic year",
            "division",
            "grade",
            "university seal",
            "registrar signature",
            "degree verification",
        ],
        "hi": [
            "डिग्री प्रमाणपत्र",
            "स्नातक प्रमाणपत्र",
            "स्नातक डिग्री",
            "स्नातकोत्तर डिग्री",
            "डॉक्टरेट डिग्री",
            "अनंतिम प्रमाणपत्र",
            "दीक्षांत प्रमाणपत्र",
            "विश्वविद्यालय का नाम",
            "पंजीकरण संख्या",
            "प्रमाणपत्र संख्या",
            "प्रदत्त डिग्री",
            "विशेषज्ञता",
            "स्नातक तिथि",
            "दीक्षांत तिथि",
            "शैक्षणिक वर्ष",
            "श्रेणी",
            "ग्रेड",
            "विश्वविद्यालय मुहर",
            "कुलसचिव हस्ताक्षर",
            "डिग्री सत्यापन",
        ],
        # Add similar entries for ta, te, kn, mr, gu
    }

    VALID_DEGREES = {
        "BE",     # Bachelor of Engineering
        "BTECH",  # Bachelor of Technology
        "BSC",    # Bachelor of Science
        "BCOM",   # Bachelor of Commerce
        "BA",     # Bachelor of Arts
        "BBA",    # Bachelor of Business Administration
        "BCA",    # Bachelor of Computer Applications
        "ME",     # Master of Engineering
        "MTECH",  # Master of Technology
        "MSC",    # Master of Science
        "MCOM",   # Master of Commerce
        "MA",     # Master of Arts
        "MBA",    # Master of Business Administration
        "MCA",    # Master of Computer Applications
        "MBBS",   # Bachelor of Medicine and Surgery
        "BDS",    # Bachelor of Dental Surgery
        "BAMS",   # Bachelor of Ayurvedic Medicine
        "BHMS",   # Bachelor of Homeopathic Medicine
        "LLB",    # Bachelor of Laws
        "PHD",    # Doctor of Philosophy
        "DPHIL",  # Doctor of Philosophy
    }

    VALID_UNIVERSITIES = {
        "MU",    # Mumbai University
        "DU",    # Delhi University
        "BU",    # Bombay University
        "AU",    # Anna University
        "PU",    # Pune University
        "KU",    # Karnataka University
        "OU",    # Osmania University
        "CU",    # Calcutta University
        "GU",    # Gujarat University
        "JNU",   # Jawaharlal Nehru University
    }

    def __init__(
        self,
        language: str = "en",
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_entity: str = "IN_DEGREE_CERTIFICATE",
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
        return self.__check_degree_reference(sanitized_value)

    def __check_degree_reference(self, sanitized_value: str) -> bool:
        """
        Validate degree certificate reference formats.
        
        Checks:
        1. Length validation
        2. Degree code validation
        3. University code validation
        4. Year format validation
        5. Number format validation
        6. Certificate type validation
        """
        if len(sanitized_value) < 8 or len(sanitized_value) > 20:
            return False

        # Check for valid degree prefixes
        for degree in self.VALID_DEGREES:
            if sanitized_value.upper().startswith(degree):
                remaining = sanitized_value[len(degree):]
                # Check if remaining part contains a valid university code
                for univ in self.VALID_UNIVERSITIES:
                    if remaining.startswith(univ):
                        number_part = remaining[len(univ):]
                        return number_part.isdigit() and len(number_part) >= 6

        # Check for certificate number format
        if "/" in sanitized_value:
            parts = sanitized_value.split("/")
            if len(parts) == 4:
                return (
                    parts[0] in self.VALID_UNIVERSITIES and
                    parts[1] in ["DEG", "PROV", "CONV"] and
                    parts[2].isdigit() and len(parts[2]) == 2 and
                    parts[3].isdigit() and len(parts[3]) >= 4
                )

        # Check for registration number format
        if len(sanitized_value) >= 10:
            univ_code = sanitized_value[:2]
            year_part = sanitized_value[2:4]
            if (univ_code in self.VALID_UNIVERSITIES and
                year_part.isdigit() and
                0 <= int(year_part) <= 99):
                return sanitized_value[4:].isalnum()

        # General alphanumeric validation
        return (
            sanitized_value.isalnum() and
            any(char.isdigit() for char in sanitized_value) and
            any(char.isalpha() for char in sanitized_value)
        )