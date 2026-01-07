"""
TMX Corpus Extraction Module

This module provides memory-efficient extraction of sentence pairs
from TMX files with configurable filtering criteria.
"""

import csv
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator, Tuple, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Container for extraction results and metadata."""
    output_path: Path
    total_units: int
    matched_units: int
    keywords_matched: dict


class TMXExtractor:
    """
    Memory-efficient TMX file processor for parallel corpora.
    
    Attributes:
        source_lang: Source language code (e.g., 'eu' for Basque)
        target_lang: Target language code (e.g., 'es' for Spanish)
    """
    
    def __init__(self, source_lang: str = 'eu', target_lang: str = 'es'):
        self.source_lang = source_lang.lower()
        self.target_lang = target_lang.lower()
        
    def _get_language(self, tuv_element: ET.Element) -> str:
        """Extract language code from TMX translation unit variant."""
        lang = (
            tuv_element.get('{http://www.w3.org/XML/1998/namespace}lang') 
            or tuv_element.get('lang', '')
        )
        return lang.lower()
    
    def _extract_text(self, tuv_element: ET.Element) -> Optional[str]:
        """Extract and clean text from TMX segment."""
        seg = tuv_element.find('seg')
        if seg is None:
            return None
            
        # Handle various text structures in TMX
        text = seg.text
        if text is None:
            text = ''.join(seg.itertext())
        
        return text.strip() if text else None
    
    def iterate_pairs(
        self, 
        tmx_path: Path
    ) -> Generator[Tuple[str, str], None, None]:
        """
        Iterate through all translation unit pairs in TMX file.
        
        Yields:
            Tuple of (source_text, target_text)
        """
        context = ET.iterparse(str(tmx_path), events=('end',))
        
        for event, elem in context:
            if elem.tag != 'tu':
                continue
                
            source_text = None
            target_text = None
            
            for tuv in elem.findall('tuv'):
                lang = self._get_language(tuv)
                text = self._extract_text(tuv)
                
                if text:
                    if self.source_lang in lang:
                        source_text = text
                    elif self.target_lang in lang:
                        target_text = text
            
            if source_text and target_text:
                yield (source_text, target_text)
            
            # Memory management
            elem.clear()
    
    def extract_with_keywords(
        self,
        tmx_path: Path,
        output_path: Path,
        keywords: List[str],
        case_sensitive: bool = False,
        match_column: str = 'source'  # 'source', 'target', or 'both'
    ) -> ExtractionResult:
        """
        Extract sentence pairs matching specified keywords.
        
        Args:
            tmx_path: Path to input TMX file
            output_path: Path for output CSV
            keywords: List of keywords to search for
            case_sensitive: Whether matching is case-sensitive
            match_column: Which column to search in
            
        Returns:
            ExtractionResult with extraction statistics
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        total_units = 0
        matched_units = 0
        keyword_counts = {kw: 0 for kw in keywords}
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                f'Source ({self.source_lang})',
                f'Target ({self.target_lang})',
                'Matched_Keywords'
            ])
            
            for source_text, target_text in self.iterate_pairs(tmx_path):
                total_units += 1
                
                # Determine search text
                if match_column == 'source':
                    search_text = source_text
                elif match_column == 'target':
                    search_text = target_text
                else:
                    search_text = f"{source_text} {target_text}"
                
                if not case_sensitive:
                    search_text = search_text.lower()
                
                # Check for keyword matches
                matched_keywords = []
                for kw in keywords:
                    check_kw = kw if case_sensitive else kw.lower()
                    if check_kw in search_text:
                        matched_keywords.append(kw)
                        keyword_counts[kw] += 1
                
                if matched_keywords:
                    writer.writerow([
                        source_text, 
                        target_text, 
                        '|'.join(matched_keywords)
                    ])
                    matched_units += 1
                
                # Progress logging
                if total_units % 500000 == 0:
                    logger.info(f"Processed {total_units:,} units, "
                               f"found {matched_units:,} matches")
        
        logger.info(f"Extraction complete: {matched_units:,}/{total_units:,} "
                   f"units matched")
        
        return ExtractionResult(
            output_path=output_path,
            total_units=total_units,
            matched_units=matched_units,
            keywords_matched=keyword_counts
        )
