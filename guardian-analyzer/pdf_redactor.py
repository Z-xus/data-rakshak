import fitz
import os
import re
from guardian_analyzer import AnalyzerEngine
from guardian_analyzer.nlp_engine import NlpEngineProvider
import logging
from typing import List, Dict, Any

class GuardianPDFRedactor:
    def __init__(self, analyzer_engine: AnalyzerEngine = None):
        """Initialize redactor with default settings"""
        self.analyzer = analyzer_engine or AnalyzerEngine()
        self.logger = logging.getLogger("guardian-pdf-redactor")

    def extract_entities_from_analysis(self, analyzer_results, text: str) -> List[str]:
        """Extract actual text strings from analyzer results"""
        entities = []
        for result in analyzer_results:
            entity_text = text[result.start : result.end]
            entities.append(entity_text)
        return list(set(entities))

    def redact_pdf(
        self,
        input_pdf: str,
        output_pdf: str,
        language: str = "en",
        additional_keywords: List[str] = None,
        custom_regex: List[str] = None,
    ) -> Dict[str, Any]:
        """Simple, sequential PDF redaction method"""
        doc = None
        try:
            if not os.path.exists(input_pdf):
                raise FileNotFoundError(f"Input PDF not found: {input_pdf}")
            
            os.makedirs(os.path.dirname(output_pdf), exist_ok=True)
            
            self.logger.info(f"Opening PDF: {input_pdf}")
            doc = fitz.open(input_pdf)
            if doc.is_closed:
                raise ValueError("Document failed to open properly")

            file_size_mb = os.path.getsize(input_pdf) / (1024 * 1024)
            self.logger.info(f"Processing PDF ({file_size_mb:.2f}MB)")

            # Get all text from PDF
            full_text = ""
            for page in doc:
                full_text += page.get_text()

            # Analyze entire text at once
            analyzer_results = self.analyzer.analyze(text=full_text, language=language)
            detected_entities = self.extract_entities_from_analysis(analyzer_results, full_text)

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

            # Process each page sequentially
            for page_num in range(len(doc)):
                page = doc[page_num]
                # Find and redact each entity
                for entity in all_entities:
                    text_instances = page.search_for(entity)
                    for inst in text_instances:
                        page.draw_rect(inst, color=(0, 0, 0), fill=(0, 0, 0))

            # Save document
            doc.save(output_pdf)

            result = {
                "status": "success",
                "file_size_mb": file_size_mb,
                "pages_processed": len(doc),
                "entities_detected": list(all_entities),
                "output_path": output_pdf,
            }
            
            self.logger.info("PDF processing completed successfully")
            return result

        except Exception as e:
            self.logger.error(f"Error during redaction: {str(e)}")
            raise

        finally:
            if doc and not doc.is_closed:
                try:
                    self.logger.info("Closing PDF document")
                    doc.close()
                except Exception as e:
                    self.logger.warning(f"Error closing document: {str(e)}")
