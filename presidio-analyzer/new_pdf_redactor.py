import io
import re
import fitz
from PIL import Image
import cv2
import numpy as np
from typing import List, Dict, Any
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import os
import multiprocessing


class PresidioPDFRedactor:
    def __init__(self, analyzer_engine: AnalyzerEngine = None):
        """
        PDF Redactor that uses Presidio analysis results
        """
        self.analyzer = analyzer_engine or AnalyzerEngine()
        self.logger = logging.getLogger("presidio-pdf-redactor")
        self.CHUNK_SIZE = 900000  # Just under spaCy's 1M limit
        self.entity_cache = {}

    def extract_entities_from_analysis(self, analyzer_results, text: str) -> List[str]:
        """Extract actual text strings from analyzer results"""
        entities = []
        for result in analyzer_results:
            entity_text = text[result.start : result.end]
            entities.append(entity_text)
        return list(set(entities))  # Remove duplicates

    def analyze_text_chunk(self, text: str, language: str) -> List[str]:
        """Analyze a chunk of text and return unique entities"""
        try:
            analyzer_results = self.analyzer.analyze(text=text, language=language)
            return self.extract_entities_from_analysis(analyzer_results, text)
        except Exception as e:
            self.logger.error(f"Error analyzing chunk: {e}")
            return []

    def analyze_document(self, doc: fitz.Document, language: str) -> List[str]:
        """Analyze document in chunks to manage memory"""
        all_entities = set()
        full_text = ""
        chunk_texts = []
        
        # Collect text and split into chunks
        for page in doc:
            text = page.get_text()
            full_text += text
            
            while len(full_text) >= self.CHUNK_SIZE:
                # Find last period or newline to split naturally
                split_pos = full_text.rfind('.', 0, self.CHUNK_SIZE)
                if split_pos == -1:
                    split_pos = full_text.rfind('\n', 0, self.CHUNK_SIZE)
                if split_pos == -1:
                    split_pos = self.CHUNK_SIZE
                
                chunk_texts.append(full_text[:split_pos])
                full_text = full_text[split_pos:]
        
        if full_text:
            chunk_texts.append(full_text)

        # Process chunks in parallel
        with ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(self.analyze_text_chunk, chunk, language)
                for chunk in chunk_texts
            ]
            
            with tqdm(total=len(chunk_texts), desc="Analyzing text chunks") as pbar:
                for future in as_completed(futures):
                    entities = future.result()
                    all_entities.update(entities)
                    pbar.update(1)

        return list(all_entities)

    def process_page(self, args: tuple) -> Dict:
        """Process a single page in parallel"""
        page_num, page_text, entities_to_redact = args
        try:
            matches = []
            # Process entities in smaller batches to avoid regex memory issues
            BATCH_SIZE = 100
            for i in range(0, len(entities_to_redact), BATCH_SIZE):
                batch = entities_to_redact[i:i + BATCH_SIZE]
                for entity in batch:
                    matches.extend(
                        [(m.start(), m.end()) 
                         for m in re.finditer(re.escape(entity), page_text)]
                    )
            return {"page_num": page_num, "matches": matches}
        except Exception as e:
            self.logger.error(f"Error processing page {page_num}: {e}")
            return {"page_num": page_num, "matches": []}

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
        """Main redaction method with proper error handling"""
        try:
            doc = fitz.open(pdf_path)
            total_pages = len(doc)

            # Get file size for logging
            file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
            self.logger.info(f"Processing PDF ({file_size_mb:.2f}MB)")

            # Analyze document
            detected_entities = self.analyze_document(doc, language)

            # Combine with additional keywords
            all_entities = set(detected_entities)
            if additional_keywords:
                all_entities.update(additional_keywords)

            # Add custom regex patterns
            if custom_regex:
                for pattern in custom_regex:
                    for page in doc:
                        text = page.get_text()
                        matches = re.finditer(pattern, text)
                        for match in matches:
                            all_entities.add(text[match.start() : match.end()])

            # Process pages in parallel
            cpu_count = multiprocessing.cpu_count()
            page_texts = [
                (i, doc[i].get_text(), list(all_entities)) for i in range(total_pages)
            ]

            results = []
            with tqdm(total=total_pages, desc="Processing pages") as pbar:
                with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                    futures = [
                        executor.submit(self.process_page, args) for args in page_texts
                    ]

                    for future in as_completed(futures):
                        results.append(future.result())
                        pbar.update(1)

            # Apply redactions
            with tqdm(total=len(results), desc="Applying redactions") as pbar:
                for result in results:
                    page_num = result["page_num"]
                    page = doc[page_num]

                    for start, end in result["matches"]:
                        text = page.get_text()
                        text_instances = page.search_for(text[start:end])
                        for inst in text_instances:
                            page.draw_rect(inst, color=(0, 0, 0), fill=(0, 0, 0))

                    pbar.update(1)

            # Save redacted document
            doc.save(output_path)
            doc.close()

            return {
                "status": "success",
                "file_size_mb": file_size_mb,
                "pages_processed": total_pages,
                "entities_detected": list(all_entities),
                "output_path": output_path,
            }

        except Exception as e:
            self.logger.error(f"Error during redaction: {e}")
            raise e
