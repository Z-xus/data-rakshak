import io
import re
import fitz
from PIL import Image
import cv2
import numpy as np
from typing import List, Dict, Any, Generator
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import multiprocessing


class PresidioPDFRedactor:
    def __init__(self, analyzer_engine: AnalyzerEngine = None):
        """PDF Redactor with secure redaction and batch processing"""
        self.analyzer = analyzer_engine or AnalyzerEngine()
        self.logger = logging.getLogger("presidio-pdf-redactor")
        self.batch_size = 10  # Pages per batch
        self.max_workers = min(32, multiprocessing.cpu_count() * 2)

    def process_page_batch(self, batch_data: tuple) -> List[Dict]:
        """Process a batch of pages"""
        pages, entities, patterns = batch_data
        results = []

        for page_num, page_text in pages:
            try:
                # Analyze text with Presidio
                analyzer_results = self.analyzer.analyze(text=page_text, language="en")

                # Get redaction areas
                redaction_areas = []

                # From analyzer results
                for result in analyzer_results:
                    text = page_text[result.start : result.end]
                    redaction_areas.extend(
                        self.get_redaction_areas(page_num, text, page_text)
                    )

                # From provided entities
                for entity in entities:
                    redaction_areas.extend(
                        self.get_redaction_areas(page_num, entity, page_text)
                    )

                # From regex patterns
                for pattern in patterns:
                    matches = re.finditer(pattern, page_text)
                    for match in matches:
                        text = page_text[match.start() : match.end()]
                        redaction_areas.extend(
                            self.get_redaction_areas(page_num, text, page_text)
                        )

                results.append(
                    {"page_num": page_num, "redaction_areas": redaction_areas}
                )

            except Exception as e:
                self.logger.error(f"Error processing page {page_num}: {e}")
                results.append({"page_num": page_num, "redaction_areas": []})

        return results

    def get_redaction_areas(
        self, page_num: int, text: str, page_text: str
    ) -> List[Dict]:
        """Get precise redaction areas with context"""
        areas = []
        try:
            for match in re.finditer(re.escape(text), page_text):
                start_pos = match.start()
                end_pos = match.end()

                # Get surrounding context
                context_before = page_text[max(0, start_pos - 50) : start_pos]
                context_after = page_text[end_pos : min(end_pos + 50, len(page_text))]

                areas.append(
                    {
                        "text": text,
                        "start": start_pos,
                        "end": end_pos,
                        "context_before": context_before,
                        "context_after": context_after,
                    }
                )

        except Exception as e:
            self.logger.error(
                f"Error getting redaction areas for '{text}' on page {page_num}: {e}"
            )

        return areas

    def apply_secure_redaction(self, page: fitz.Page, area: Dict) -> None:
        """Apply secure redaction that prevents text selection"""
        try:
            # Get text instances with context
            text_to_find = area["text"]
            instances = page.search_for(text_to_find)

            for inst in instances:
                # Create slightly larger rectangle for complete coverage
                rect = fitz.Rect(inst)
                rect.x0 -= 1
                rect.y0 -= 1
                rect.x1 += 1
                rect.y1 += 1

                # First remove the text content
                page.add_redact_annot(rect)
                page.apply_redactions()

                # Then cover with black rectangle
                page.draw_rect(rect, color=(0, 0, 0), fill=(0, 0, 0), overlay=True)

                # Add an invisible text layer to prevent selection
                page.insert_text(
                    rect.tl,  # top-left point
                    " " * len(text_to_find),  # spaces instead of original text
                    fontsize=0.1,
                    color=(0, 0, 0),
                )

        except Exception as e:
            self.logger.error(f"Error applying secure redaction: {e}")

    def redact_pdf(
        self,
        pdf_path: str,
        output_path: str,
        language: str = "en",
        additional_keywords: List[str] = None,
        custom_regex: List[str] = None,
    ) -> Dict[str, Any]:
        """Main redaction method with batch processing"""
        try:
            doc = fitz.open(pdf_path)
            total_pages = len(doc)

            # Prepare batches
            all_pages = []
            with tqdm(total=total_pages, desc="Preparing pages") as pbar:
                for page_num in range(total_pages):
                    page_text = doc[page_num].get_text()
                    all_pages.append((page_num, page_text))
                    pbar.update(1)

            # Create batches
            batches = [
                all_pages[i : i + self.batch_size]
                for i in range(0, len(all_pages), self.batch_size)
            ]

            # Process batches in parallel
            results = []
            patterns = custom_regex or [r"\b[A-Z]{2}\d{6}\b"]
            keywords = additional_keywords or []

            with tqdm(total=len(batches), desc="Processing batches") as pbar:
                with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(
                            self.process_page_batch, (batch, keywords, patterns)
                        )
                        for batch in batches
                    ]

                    for future in as_completed(futures):
                        results.extend(future.result())
                        pbar.update(1)

            # Apply secure redactions
            with tqdm(total=len(results), desc="Applying redactions") as pbar:
                for result in results:
                    page = doc[result["page_num"]]
                    for area in result["redaction_areas"]:
                        self.apply_secure_redaction(page, area)
                    pbar.update(1)

            # Save document
            doc.save(
                output_path,
                garbage=4,  # Maximum cleanup
                deflate=True,  # Compress content
                clean=True,  # Remove unused elements
            )
            doc.close()

            return {
                "status": "success",
                "pages_processed": total_pages,
                "output_path": output_path,
            }

        except Exception as e:
            self.logger.error(f"Error during redaction: {e}")
            raise
