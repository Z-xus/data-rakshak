import spacy
import fitz
import os
import re
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import NlpEngineProvider
import logging
from typing import List, Dict, Any, Generator
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
from tqdm import tqdm
from collections import deque

class PresidioPDFRedactor:
    def __init__(self, analyzer_engine: AnalyzerEngine = None):
        """Initialize redactor with default settings"""
        self.analyzer = analyzer_engine or AnalyzerEngine()
        self.logger = logging.getLogger("presidio-pdf-redactor")
        self.chunk_size = 50000  # Process 50K chars at a time
        self.overlap = 5000      # 5K char overlap for context
        self.context_window = 3  # Pages to keep in context

    def process_text_chunk(self, text: str, language: str) -> List[str]:
        """Process a single chunk of text"""
        try:
            results = self.analyzer.analyze(
                text=text,
                language=language
            )
            return [text[result.start:result.end] for result in results]
        except Exception as e:
            self.logger.error(f"Error processing chunk: {e}")
            return []

    def chunk_document(self, doc: fitz.Document) -> Generator[tuple, None, None]:
        """Generate chunks of text with context"""
        context_buffer = deque(maxlen=self.context_window)
        current_text = ""
        current_page = 0
        
        while current_page < len(doc):
            # Get text from current page
            page_text = doc[current_page].get_text()
            context_buffer.append(page_text)
            
            # Combine with current text
            current_text += page_text
            
            # Process chunks when we have enough text
            while len(current_text) >= self.chunk_size:
                # Find a good breaking point (sentence end if possible)
                break_point = self.chunk_size
                if '.' in current_text[break_point-100:break_point+100]:
                    break_point = current_text.find('.', break_point-100, break_point+100) + 1
                
                # Get chunk with context
                chunk = current_text[:break_point]
                context_before = " ".join(list(context_buffer)[:-1])
                context_after = (
                    doc[current_page + 1].get_text() 
                    if current_page + 1 < len(doc) else ""
                )
                
                yield (
                    chunk,
                    context_before[-self.overlap:] if context_before else "",
                    context_after[:self.overlap] if context_after else "",
                    current_page
                )
                
                # Keep overlap for next chunk
                current_text = current_text[break_point-self.overlap:]
            
            current_page += 1
        
        # Process remaining text
        if current_text:
            yield (
                current_text,
                context_buffer[-2].get_text() if len(context_buffer) > 1 else "",
                "",
                current_page - 1
            )

    def analyze_document(self, doc: fitz.Document, language: str) -> List[str]:
        """Analyze document in chunks"""
        all_entities = set()
        
        for chunk, context_before, context_after, page_num in self.chunk_document(doc):
            # Analyze text with context
            analysis_text = f"{context_before} {chunk} {context_after}"
            chunk_entities = self.process_text_chunk(analysis_text, language)
            
            # Add found entities
            all_entities.update(chunk_entities)
            
            self.logger.info(f"Processed chunk on page {page_num}, "
                           f"found {len(chunk_entities)} entities")
        
        return list(all_entities)

    def redact_pdf(
        self,
        pdf_path: str,
        output_path: str,
        language: str = "en",
        additional_keywords: List[str] = None,
        custom_regex: List[str] = None
    ) -> Dict[str, Any]:
        """Main redaction method"""
        try:
            doc = fitz.open(pdf_path)
            total_pages = len(doc)
            
            # Get file size for logging
            file_size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
            self.logger.info(f"Processing PDF ({file_size_mb:.2f}MB)")
            
            # Analyze document in chunks
            detected_entities = self.analyze_document(doc, language)
            
            # Combine with additional keywords
            all_entities = set(detected_entities)
            if additional_keywords:
                all_entities.update(additional_keywords)
            
            # Add regex matches if provided
            if custom_regex:
                for pattern in custom_regex:
                    for page in doc:
                        text = page.get_text()
                        matches = re.finditer(pattern, text)
                        for match in matches:
                            all_entities.add(text[match.start():match.end()])

            # Process pages in parallel
            cpu_count = multiprocessing.cpu_count()
            with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                futures = []
                for page_num in range(total_pages):
                    page = doc[page_num]
                    text = page.get_text()
                    futures.append(
                        executor.submit(
                            self.process_page,
                            (page_num, text, list(all_entities))
                        )
                    )
                
                # Process results with progress bar
                with tqdm(total=total_pages, desc="Processing pages") as pbar:
                    for future in as_completed(futures):
                        result = future.result()
                        page_num = result['page_num']
                        
                        # Apply redactions
                        page = doc[page_num]
                        for start, end in result['matches']:
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
                "output_path": output_path
            }
            
        except Exception as e:
            self.logger.error(f"Error during redaction: {e}")
            raise

    def process_page(self, args: tuple) -> Dict:
        """Process a single page for redaction"""
        page_num, text, entities = args
        try:
            matches = []
            for entity in entities:
                matches.extend([
                    (m.start(), m.end()) 
                    for m in re.finditer(re.escape(entity), text)
                ])
            return {
                'page_num': page_num,
                'matches': matches
            }
        except Exception as e:
            self.logger.error(f"Error processing page {page_num}: {e}")
            return {'page_num': page_num, 'matches': []}
