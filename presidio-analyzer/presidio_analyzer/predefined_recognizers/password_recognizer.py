from typing import List, Optional
import re

from presidio_analyzer import Pattern, PatternRecognizer


class PasswordRecognizer(PatternRecognizer):
    """
    Recognizes password fields using regex and context.
    """

    PATTERNS = [
        Pattern(
            "Strong Password",
            r"\b(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[$!%*?&#])[A-Za-z\d$!%*?&#]{8,32}\b",
            0.1,
        ),
        Pattern(
            "Medium Password",
            r"\b(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d$!%*?&#]{6,32}\b",
            0.05,
        ),
    ]

    CONTEXT = [
        "password", "pwd", "passcode", "secret",
        "credentials", "login", "authentication",
        "account details", "secure", "access", "access code",
        "access key", "access token", "access code", "access key",
    ]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
        supported_entity: str = "PASSWORD",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity=supported_entity,
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def validate_result(self, pattern_text: str) -> bool:
        """
        Validates that the pattern match is actually a password.
        """
        # Skip common words and email patterns
        if '@' in pattern_text or pattern_text.lower() in ["password", "credentials"]:
            return False

        # Require at least one number and one special char
        has_digit = bool(re.search(r'\d', pattern_text))
        has_special = bool(re.search(r'[$!%*?&#]', pattern_text))
        
        return has_digit and has_special