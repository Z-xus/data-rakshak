import io
import re
import fitz
import logging
import concurrent.futures
import multiprocessing
import os
from typing import List, Dict, Any
from presidio_analyzer import AnalyzerEngine


class PresidioPDFRedactor:
    def __init__(self, analyzer_engine: AnalyzerEngine = None):
        """
        PDF Redactor that uses Presidio analysis results with multiprocessing optimization
        """
        self.analyzer = analyzer_engine or AnalyzerEngine()

        # Precompile common regex patterns for efficiency
        self.default_regex_patterns = [
            r"\b[A-Z]{2}\d{6}\b",  # Default ID-like pattern
            r"\b\d{3}-\d{2}-\d{4}\b",  # SSN pattern
            r"\b\d{16}\b",  # Credit card-like pattern
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b",  # Email pattern
        ]

    def extract_entities_from_analysis(self, analyzer_results, text: str) -> List[str]:
        """Extract actual text strings from analyzer results"""
        entities = []
        for result in analyzer_results:
            entity_text = text[result.start : result.end]
            if len(entity_text.strip()) > 2:
                entities.append(entity_text)
        return list(set(entities))  # Remove duplicates

    def process_page(self, page_data):
        """
        Process a single page for redaction with enhanced performance
        """
        page_text, page_num, redaction_config = page_data
        matches = []
        try:
            # Process keywords
            for keyword in redaction_config["keywords"]:
                for match in re.finditer(re.escape(keyword), page_text):
                    matches.append((match.start(), match.end()))

            # Process regex patterns
            for pattern in redaction_config["regex_patterns"]:
                for match in re.finditer(pattern, page_text):
                    matches.append((match.start(), match.end()))

            return page_num, matches

        except Exception as e:
            print(f"Error processing page {page_num}: {e}")
            return page_num, []

    def redact_pdf(
        self,
        pdf_path: str,
        output_path: str,
        language: str = "en",
        additional_keywords: List[str] = None,
        custom_regex: List[str] = None,
        entities: List[str] = None,
    ) -> Dict[str, Any]:
        """
        Analyze and redact PDF using Presidio analysis with multiprocessing
        """
        # Open document
        doc = fitz.open(pdf_path)
        detected_entities = set()

        # First pass: Entity Detection
        for page_num in range(len(doc)):
            page = doc[page_num]
            page_text = page.get_text()

            # Analyze text with Presidio
            analyzer_results = self.analyzer.analyze(
                text=page_text, 
                language=language,
                entities=entities
            )
            print(f"Page {page_num + 1} - Detected entities: {analyzer_results}")

            # Extract entities from analysis
            page_entities = self.extract_entities_from_analysis(
                analyzer_results, page_text
            )
            detected_entities.update(page_entities)

        # Prepare redaction configuration
        redaction_config = {
            "keywords": list(detected_entities),
            "regex_patterns": self.default_regex_patterns.copy(),
        }

        # Add additional keywords and patterns
        if additional_keywords:
            redaction_config["keywords"].extend(additional_keywords)
        if custom_regex:
            redaction_config["regex_patterns"].extend(custom_regex)

        # Multiprocessing Redaction
        num_cores = max(1, multiprocessing.cpu_count() - 1)
        processed_pages = []

        with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
            # Prepare page processing tasks with text instead of page objects
            page_tasks = [
                (doc[page_num].get_text(), page_num, redaction_config)
                for page_num in range(len(doc))
            ]

            # Submit tasks and collect results
            futures = {
                executor.submit(self.process_page, task): task[1] for task in page_tasks
            }

            for future in concurrent.futures.as_completed(futures):
                page_num, matches = future.result()
                if matches:
                    # Apply redactions to the original document
                    page = doc[page_num]
                    for start, end in matches:
                        text = page.get_text()[start:end]
                        instances = page.search_for(text)
                        for inst in instances:
                            page.draw_rect(inst, color=(0, 0, 0), fill=(0, 0, 0))
                    processed_pages.append(page_num)

        # Save the redacted document
        doc.save(output_path)
        doc.close()

        return {
            "status": "success",
            "detected_entities": list(detected_entities),
            "output_path": output_path,
        }
