import os
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from rapidfuzz import fuzz, process
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import time
import logging
from typing import List, Dict, Tuple
import multiprocessing as mp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Robust project-root-relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
ABR_INPUT = os.path.join(PROJECT_ROOT, 'data', 'abr', 'cleaned', 'companies_cleaned.csv')
CC_INPUT = os.path.join(PROJECT_ROOT, 'data', 'common_crawl', 'cleaned', 'companies_cleaned.csv')
OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'entity_matching', 'entity_matches_optimized.csv')

class OptimizedEntityMatcher:
    def __init__(self, batch_size: int = 1000, max_workers: int = None):
        """
        Initialize the optimized entity matcher
        
        Args:
            batch_size: Number of records to process in each batch
            max_workers: Maximum number of worker processes/threads
        """
        self.batch_size = batch_size
        self.max_workers = max_workers or min(mp.cpu_count(), 8)
        self.matches = []
        
    def normalize_name(self, name: str) -> str:
        """Normalize company name for comparison"""
        if pd.isna(name):
            return ""
        return str(name).lower().strip()
    
    def create_blocks(self, df: pd.DataFrame, block_size: int = 2) -> Dict[str, pd.DataFrame]:
        """
        Create blocks for efficient matching based on first N characters
        
        Args:
            df: DataFrame to block
            block_size: Number of characters to use for blocking
            
        Returns:
            Dictionary of blocked DataFrames
        """
        blocks = {}
        for idx, row in df.iterrows():
            name = self.normalize_name(row['norm_name'])
            if len(name) >= block_size:
                block_key = name[:block_size]
                if block_key not in blocks:
                    blocks[block_key] = []
                blocks[block_key].append(row)
        
        # Convert lists to DataFrames
        return {k: pd.DataFrame(v) for k, v in blocks.items()}
    
    def fuzzy_match_batch(self, cc_batch: pd.DataFrame, abr_blocks: Dict[str, pd.DataFrame]) -> List[Dict]:
        """
        Perform fuzzy matching on a batch of Common Crawl companies
        
        Args:
            cc_batch: Batch of Common Crawl companies
            abr_blocks: Blocked ABR companies
            
        Returns:
            List of matches found
        """
        batch_matches = []
        
        for _, cc_row in cc_batch.iterrows():
            cc_name = self.normalize_name(cc_row['norm_name'])
            if len(cc_name) < 2:
                continue
                
            # Get relevant blocks
            block_key = cc_name[:2]
            relevant_blocks = []
            
            # Check exact block and neighboring blocks
            for key in abr_blocks.keys():
                if key.startswith(block_key) or block_key.startswith(key):
                    relevant_blocks.append(abr_blocks[key])
            
            if not relevant_blocks:
                continue
                
            # Combine relevant blocks
            abr_candidates = pd.concat(relevant_blocks, ignore_index=True)
            
            # Use rapidfuzz.process for efficient matching
            matches = process.extract(
                cc_name, 
                abr_candidates['norm_name'].tolist(),
                limit=5,  # Get top 5 matches
                scorer=fuzz.token_sort_ratio
            )
            
            # Filter by threshold
            for match_name, score in matches:
                if score >= 85:
                    abr_row = abr_candidates[abr_candidates['norm_name'] == match_name].iloc[0]
                    batch_matches.append({
                        'cc_company': cc_row['company_name'],
                        'abr_company': abr_row['entity_name'],
                        'cc_abn': cc_row.get('abn', ''),
                        'abr_abn': abr_row.get('abn', ''),
                        'score': score,
                        'method': 'fuzzy_optimized'
                    })
        
        return batch_matches
    
    def tfidf_match_batch(self, cc_batch: pd.DataFrame, abr_blocks: Dict[str, pd.DataFrame]) -> List[Dict]:
        """
        Perform TF-IDF matching on a batch of companies
        
        Args:
            cc_batch: Batch of Common Crawl companies
            abr_blocks: Blocked ABR companies
            
        Returns:
            List of matches found
        """
        batch_matches = []
        
        # Get all relevant ABR names
        all_abr_names = []
        for block_df in abr_blocks.values():
            all_abr_names.extend(block_df['norm_name'].tolist())
        
        if not all_abr_names:
            return batch_matches
        
        # Create TF-IDF vectors
        cc_names = cc_batch['norm_name'].tolist()
        vectorizer = TfidfVectorizer(
            max_features=10000,  # Limit features for memory efficiency
            ngram_range=(1, 2),  # Use unigrams and bigrams
            min_df=1,
            max_df=0.95
        )
        
        try:
            # Fit on ABR names and transform both
            all_names = all_abr_names + cc_names
            tfidf_matrix = vectorizer.fit_transform(all_names)
            
            # Split matrix
            abr_matrix = tfidf_matrix[:len(all_abr_names)]
            cc_matrix = tfidf_matrix[len(all_abr_names):]
            
            # Calculate similarities
            similarities = cosine_similarity(cc_matrix, abr_matrix)
            
            # Find best matches
            for i, cc_name in enumerate(cc_names):
                best_idx = similarities[i].argmax()
                best_score = similarities[i][best_idx]
                
                if best_score >= 0.7:
                    cc_row = cc_batch.iloc[i]
                    abr_name = all_abr_names[best_idx]
                    
                    # Find corresponding ABR row
                    for block_df in abr_blocks.values():
                        abr_row = block_df[block_df['norm_name'] == abr_name]
                        if not abr_row.empty:
                            abr_row = abr_row.iloc[0]
                            batch_matches.append({
                                'cc_company': cc_row['company_name'],
                                'abr_company': abr_row['entity_name'],
                                'cc_abn': cc_row.get('abn', ''),
                                'abr_abn': abr_row.get('abn', ''),
                                'score': best_score,
                                'method': 'tfidf_optimized'
                            })
                            break
                            
        except Exception as e:
            logger.warning(f"TF-IDF matching failed for batch: {e}")
        
        return batch_matches
    
    def process_batch(self, cc_batch: pd.DataFrame, abr_blocks: Dict[str, pd.DataFrame]) -> List[Dict]:
        """
        Process a batch of companies using multiple matching methods
        
        Args:
            cc_batch: Batch of Common Crawl companies
            abr_blocks: Blocked ABR companies
            
        Returns:
            List of matches found
        """
        matches = []
        
        # Fuzzy matching
        fuzzy_matches = self.fuzzy_match_batch(cc_batch, abr_blocks)
        matches.extend(fuzzy_matches)
        
        # TF-IDF matching
        tfidf_matches = self.tfidf_match_batch(cc_batch, abr_blocks)
        matches.extend(tfidf_matches)
        
        return matches
    
    def match_entities(self) -> pd.DataFrame:
        """
        Main method to perform entity matching with optimizations
        
        Returns:
            DataFrame with matches
        """
        logger.info("Loading data...")
        
        # Load data with memory optimization
        abr = pd.read_csv(ABR_INPUT, dtype={
            'abn': str,
            'entity_name': str,
            'entity_type': str,
            'entity_status': str
        })
        cc = pd.read_csv(CC_INPUT, dtype={
            'website_url': str,
            'company_name': str,
            'industry': str
        })
        
        logger.info(f"Loaded {len(abr)} ABR records and {len(cc)} Common Crawl records")
        
        # Normalize names
        logger.info("Normalizing company names...")
        abr['norm_name'] = abr['entity_name'].apply(self.normalize_name)
        cc['norm_name'] = cc['company_name'].apply(self.normalize_name)
        
        # Create blocks for ABR data
        logger.info("Creating blocks for efficient matching...")
        abr_blocks = self.create_blocks(abr, block_size=2)
        logger.info(f"Created {len(abr_blocks)} blocks")
        
        # Process in batches
        logger.info(f"Processing in batches of {self.batch_size}...")
        all_matches = []
        
        # Split CC data into batches
        cc_batches = [cc[i:i + self.batch_size] for i in range(0, len(cc), self.batch_size)]
        
        start_time = time.time()
        
        # Use ThreadPoolExecutor for I/O bound operations
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit batch processing tasks
            future_to_batch = {
                executor.submit(self.process_batch, batch, abr_blocks): i 
                for i, batch in enumerate(cc_batches)
            }
            
            # Collect results
            for future in future_to_batch:
                try:
                    batch_matches = future.result()
                    all_matches.extend(batch_matches)
                    
                    batch_num = future_to_batch[future]
                    logger.info(f"Processed batch {batch_num + 1}/{len(cc_batches)} - Found {len(batch_matches)} matches")
                    
                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
        
        processing_time = time.time() - start_time
        logger.info(f"Processing completed in {processing_time:.2f} seconds")
        
        # Create results DataFrame
        if all_matches:
            matches_df = pd.DataFrame(all_matches)
            
            # Remove duplicates
            matches_df = matches_df.drop_duplicates(subset=['cc_company', 'abr_company'])
            
            # Sort by score
            matches_df = matches_df.sort_values('score', ascending=False)
            
            logger.info(f"Found {len(matches_df)} unique matches")
            
            # Save results
            os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
            matches_df.to_csv(OUTPUT_PATH, index=False)
            logger.info(f"Results saved to {OUTPUT_PATH}")
            
            return matches_df
        else:
            logger.warning("No matches found")
            return pd.DataFrame()

def main():
    """Main function to run optimized entity matching"""
    logger.info("Starting optimized entity matching...")
    
    # Initialize matcher with performance settings
    matcher = OptimizedEntityMatcher(
        batch_size=500,  # Smaller batches for memory efficiency
        max_workers=4    # Conservative worker count
    )
    
    # Run matching
    results = matcher.match_entities()
    
    if not results.empty:
        logger.info("Entity matching completed successfully!")
        logger.info(f"Total matches found: {len(results)}")
        logger.info(f"Average confidence score: {results['score'].mean():.2f}")
    else:
        logger.warning("No matches found. Check your data and thresholds.")

if __name__ == "__main__":
    main() 