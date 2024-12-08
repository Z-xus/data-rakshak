from typing import List, Optional, Tuple
import re

from presidio_analyzer import Pattern, PatternRecognizer


class PasswordRecognizer(PatternRecognizer):
    """
    Recognizes password fields using regex and context.
    Implements advanced pattern matching and scoring based on password strength analysis.
    """

    # Common password patterns people use (based on security research)
    COMMON_PATTERNS = {
        # Keyboard patterns
        r'qwerty': 0.3,
        r'asdfgh': 0.3,
        r'zxcvbn': 0.3,
        r'qwertz': 0.3,
        r'[qwertasdfgzxcvb]{6,}': 0.4,  # Keyboard walks
        
        # Number patterns
        r'123456': 0.2,
        r'654321': 0.2,
        r'\d{6,}': 0.3,  # All digits
        r'19\d{2}': 0.4,  # Years
        r'20\d{2}': 0.4,  # Years
        
        # Letter patterns
        r'password': 0.1,
        r'letmein': 0.2,
        r'admin': 0.1,
        r'welcome': 0.2,
        r'[a-z]{6,}': 0.4,  # All lowercase
        r'[A-Z]{6,}': 0.4,  # All uppercase
        
        # Special char patterns
        r'[@#$%]{2,}': 0.4,  # Multiple special chars in sequence
        r'[!@#$%^&*]+$': 0.3,  # Special chars only at end
    }

    PATTERNS = [
        Pattern(
            "Strong Password",
            # Complex pattern requiring mix of chars
            r"\b(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[$!%*?&#])[A-Za-z\d$!%*?&#]{8,32}\b",
            0.1,
        ),
        Pattern(
            "Medium Password",
            r"\b(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d$!%*?&#]{6,32}\b",
            0.05,
        ),
    ]

    CONTEXT = ["password", "pwd", "passcode"]

    def __init__(
        self,
        patterns: Optional[List[Pattern]] = None,
        context: Optional[List[str]] = None,
        supported_language: str = "en",
    ):
        patterns = patterns if patterns else self.PATTERNS
        context = context if context else self.CONTEXT
        super().__init__(
            supported_entity="PASSWORD",
            patterns=patterns,
            context=context,
            supported_language=supported_language,
        )

    def validate_result(self, pattern_text: str) -> bool:
        """
        Validates that the pattern match is actually a password.
        Implements comprehensive pattern analysis.
        """
        # Skip common words and email patterns
        if '@' in pattern_text or pattern_text.lower() in ["password", "credentials"]:
            return False

        # Basic requirements
        has_digit = bool(re.search(r'\d', pattern_text))
        has_special = bool(re.search(r'[$!%*?&#]', pattern_text))
        
        if not (has_digit and has_special):
            return False

        # Check entropy and pattern strength
        score = self._analyze_password_strength(pattern_text)
        return score > 0.5  # Only accept if strength score is above threshold

    def _analyze_password_strength(self, password: str) -> float:
        """
        Analyzes password strength based on multiple factors.
        Returns a score between 0 and 1.
        """
        base_score = 1.0
        password_lower = password.lower()

        # Check for common patterns
        for pattern, penalty in self.COMMON_PATTERNS.items():
            if re.search(pattern, password_lower):
                base_score -= penalty

        # Analyze character diversity
        char_types = [
            bool(re.search(r'[a-z]', password)),  # lowercase
            bool(re.search(r'[A-Z]', password)),  # uppercase
            bool(re.search(r'\d', password)),     # digits
            bool(re.search(r'[$!%*?&#]', password))  # special
        ]
        diversity_score = sum(char_types) / 4.0
        
        # Analyze length (8-32 chars ideal)
        length = len(password)
        if length < 8:
            length_score = 0.3
        elif length < 12:
            length_score = 0.6
        elif length < 16:
            length_score = 0.8
        else:
            length_score = 1.0

        # Check for repeating patterns
        if re.search(r'(.)\1{2,}', password):  # Same char 3+ times
            base_score -= 0.3
        
        # Check for sequential patterns
        if re.search(r'(abc|bcd|cde|def|efg|123|234|345|456|567|678|789)', password_lower):
            base_score -= 0.3

        # Combine scores with weights
        final_score = (
            base_score * 0.4 +      # Base score (after pattern penalties)
            diversity_score * 0.3 +  # Character diversity
            length_score * 0.3       # Length score
        )

        # Ensure score is between 0 and 1
        return max(0.0, min(1.0, final_score))

    def analyze(self, text: str, entities: List[str]) -> List[dict]:
        """
        Analyzes text for password patterns with strength scoring.
        """
        results = super().analyze(text, entities)
        
        # Adjust confidence scores based on password strength
        for result in results:
            if result.entity_type == "PASSWORD":
                strength_score = self._analyze_password_strength(result.text)
                # Blend original score with strength score
                result.score = (result.score + strength_score) / 2

        return results