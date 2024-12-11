import io
import re
import fitz
from PIL import Image
import cv2
import numpy as np
from typing import List, Dict, Any
from guardian_analyzer import AnalyzerEngine
from guardian_analyzer.nlp_engine import NlpEngineProvider


class GuardianPDFRedactor:
    def __init__(self, analyzer_engine: AnalyzerEngine = None):
        """
        PDF Redactor that uses Guardian analysis results
        """
        self.analyzer = analyzer_engine or AnalyzerEngine()

    def extract_entities_from_analysis(self, analyzer_results, text: str) -> List[str]:
        """Extract actual text strings from analyzer results"""
        entities = []
        for result in analyzer_results:
            entity_text = text[result.start : result.end]
            entities.append(entity_text)
        return list(set(entities))  # Remove duplicates

    def preprocess_image(self, pil_image):
        """Preprocess image for better OCR"""
        opencv_image = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
        gray = cv2.cvtColor(opencv_image, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (5, 5), 0)
        thresh = cv2.adaptiveThreshold(
            blur, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2
        )
        return Image.fromarray(cv2.cvtColor(thresh, cv2.COLOR_GRAY2RGB))

    def redact_pdf(
        self,
        pdf_path: str,
        output_path: str,
        language: str = "en",
        additional_keywords: List[str] = None,
        custom_regex: List[str] = None,
    ) -> Dict[str, Any]:
        """
        Analyze and redact PDF using Guardian analysis
        """
        doc = fitz.open(pdf_path)
        detected_entities = set()

        # Process each page
        for page_num in range(len(doc)):
            page = doc[page_num]
            page_text = page.get_text()

            # Analyze text with Presidio
            analyzer_results = self.analyzer.analyze(text=page_text, language=language)
            print(f"Page {page_num + 1} - Detected entities: {analyzer_results}")
            logger.info(f"Page {page_num + 1} - Detected entities: {analyzer_results}")

            # Extract entities from analysis
            page_entities = self.extract_entities_from_analysis(
                analyzer_results, page_text
            )
            detected_entities.update(page_entities)

        # Create redaction configuration
        redaction_config = {
            "keywords": list(detected_entities),
            "regex_patterns": [
                r"\b[A-Z]{2}\d{6}\b",  # Default patterns
            ],
        }

        # Add additional keywords and patterns
        if additional_keywords:
            redaction_config["keywords"].extend(additional_keywords)
        if custom_regex:
            redaction_config["regex_patterns"].extend(custom_regex)

        # Apply redactions
        for page_num in range(len(doc)):
            page = doc[page_num]
            page_text = page.get_text()

            # Redact keywords
            for keyword in redaction_config["keywords"]:
                text_instances = page.search_for(keyword)
                for inst in text_instances:
                    page.draw_rect(inst, color=(0, 0, 0), fill=(0, 0, 0))

            # Redact regex patterns
            for pattern in redaction_config["regex_patterns"]:
                matches = list(re.finditer(pattern, page_text))
                for match in matches:
                    text_instances = page.search_for(
                        page_text[match.start() : match.end()]
                    )
                    for inst in text_instances:
                        page.draw_rect(inst, color=(0, 0, 0), fill=(0, 0, 0))

        # Save redacted document
        doc.save(output_path)

        return {
            "status": "success",
            "detected_entities": list(detected_entities),
            "output_path": output_path,
        }
