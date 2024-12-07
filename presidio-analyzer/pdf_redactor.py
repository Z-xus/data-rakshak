import fitz
import os
import re
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
import logging
from typing import List, Dict, Any


class PresidioPDFRedactor:
    def __init__(self, analyzer_engine: AnalyzerEngine = None):
        """Initialize redactor with default settings"""
        self.analyzer = analyzer_engine or AnalyzerEngine()
        self.logger = logging.getLogger("presidio-pdf-redactor")

    def extract_entities_from_analysis(self, analyzer_results, text: str) -> List[str]:
        """Extract actual text strings from analyzer results"""
        entities = []
        for result in analyzer_results:
            entity_text = text[result.start : result.end]
            entities.append(entity_text)
        return list(set(entities))

    def redact_pdf(
        self,
        pdf_path: str,
        output_path: str,
        language: str = "en",
        additional_keywords: List[str] = None,
        custom_regex: List[str] = None,
    ) -> Dict[str, Any]:
        """Simple, sequential PDF redaction method"""
        doc = None
        try:
            # Verify input file exists
            if not os.path.exists(pdf_path):
                raise FileNotFoundError(f"Input PDF not found: {pdf_path}")

            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            self.logger.info(f"Opening PDF: {pdf_path}")
            # Open document with more explicit error handling
            doc = fitz.open(pdf_path)
            if doc.is_closed:
                raise ValueError("Document failed to open properly")

            file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
            self.logger.info(f"Processing PDF ({file_size_mb:.2f}MB)")

            # Get all text from PDF with verification
            full_text = ""
            page_count = doc.page_count  # Cache the page count
            self.logger.info(f"PDF has {page_count} pages")

            for page_num in range(page_count):
                if doc.is_closed:
                    raise ValueError(
                        f"Document closed unexpectedly while reading page {page_num}"
                    )
                page = doc[page_num]
                full_text += page.get_text()

            self.logger.info("Analyzing text with Presidio")
            # Analyze entire text at once
            analyzer_results = self.analyzer.analyze(text=full_text, language=language)
            detected_entities = self.extract_entities_from_analysis(
                analyzer_results, full_text
            )

            # print(detected_entities)

            # Combine with additional keywords
            all_entities = set(detected_entities)
            if additional_keywords:
                all_entities.update(additional_keywords)

            # Add custom regex patterns
            if custom_regex:
                for pattern in custom_regex:
                    matches = re.finditer(pattern, full_text)
                    for match in matches:
                        all_entities.add(full_text[match.start() : match.end()])

            self.logger.info(f"Found {len(all_entities)} entities to redact")
            print("Ye le")
            print(all_entities)

            # Process each page sequentially with verification
            for page_num in range(page_count):
                if doc.is_closed:
                    raise ValueError(
                        f"Document closed unexpectedly while processing page {page_num}"
                    )
                page = doc[page_num]
                # Find and redact each entity
                for entity in all_entities:
                    text_instances = page.search_for(entity)
                    for inst in text_instances:
                        page.draw_rect(inst, color=(0, 0, 0), fill=(0, 0, 0))

            # Save document
            doc.save(output_path)

            return {
                "status": "success",
                "file_size_mb": file_size_mb,
                "pages_processed": len(doc),
                "entities_detected": list(all_entities),
                "output_path": output_path,
            }

        except Exception as e:
            self.logger.error(f"Error during redaction: {e}")
            raise e

        finally:
            # Ensure document is properly closed in finally block
            if doc:
                try:
                    doc.close()
                except Exception as e:
                    self.logger.warning(f"Error closing document: {e}")
