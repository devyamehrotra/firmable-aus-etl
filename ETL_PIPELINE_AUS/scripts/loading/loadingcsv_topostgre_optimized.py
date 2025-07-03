import os
import psycopg2
import pandas as pd
import time
import logging
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
import gc
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedDataLoader:
    def __init__(self, batch_size: int = 10000, max_workers: int = 4):
        """
        Initialize the optimized data loader
        
        Args:
            batch_size: Number of records to process in each batch
            max_workers: Maximum number of worker threads
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.engine = None
        self.connection_pool = None
        
        # Get database configuration from environment variables
        self.db_config = {
            "user": os.getenv('DB_USER', 'newuser'),
            "password": os.getenv('DB_PASSWORD', 'newpassword'),
            "host": os.getenv('DB_HOST', 'localhost'),
            "port": os.getenv('DB_PORT', '5432'),
            "dbname": os.getenv('DB_NAME', 'newdb')
        }
        
        # Validate required environment variables
        if not self.db_config["password"]:
            raise ValueError("DB_PASSWORD environment variable is required")
        
    def create_engine(self) -> None:
        """Create SQLAlchemy engine with connection pooling"""
        connection_string = (
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )
        
        self.engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=10,  # Number of connections to maintain
            max_overflow=20,  # Additional connections when pool is full
            pool_pre_ping=True,  # Validate connections before use
            pool_recycle=3600,  # Recycle connections after 1 hour
            echo=False  # Set to True for SQL logging
        )
        
        logger.info("Database engine created with connection pooling")
    
    def optimize_table(self, schema: str, table_name: str) -> None:
        """
        Optimize table for better performance
        
        Args:
            schema: Database schema
            table_name: Table name
        """
        try:
            with self.engine.connect() as conn:
                # Create indexes for better query performance
                index_queries = [
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_abn ON {schema}.{table_name} (abn)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_name ON {schema}.{table_name} (entity_name)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_postcode ON {schema}.{table_name} (postcode)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_state ON {schema}.{table_name} (state)"
                ]
                
                for query in index_queries:
                    try:
                        conn.execute(text(query))
                        logger.info(f"Created index: {query}")
                    except Exception as e:
                        logger.warning(f"Index creation failed: {e}")
                
                # Analyze table for query planner
                conn.execute(text(f"ANALYZE {schema}.{table_name}"))
                logger.info(f"Analyzed table {schema}.{table_name}")
                
        except Exception as e:
            logger.error(f"Error optimizing table {schema}.{table_name}: {e}")
    
    def load_csv_batch(self, csv_path: str, schema: str, table_name: str, 
                      columns: List[str], start_row: int, end_row: int) -> int:
        """
        Load a batch of CSV data into PostgreSQL
        
        Args:
            csv_path: Path to CSV file
            schema: Database schema
            table_name: Table name
            columns: List of column names
            start_row: Starting row index
            end_row: Ending row index
            
        Returns:
            Number of rows loaded
        """
        try:
            # Read CSV chunk
            df_chunk = pd.read_csv(
                csv_path, 
                skiprows=range(1, start_row + 1),  # Skip header + previous rows
                nrows=end_row - start_row,
                dtype=str  # Load as strings for flexibility
            )
            
            if df_chunk.empty:
                return 0
            
            # Ensure columns match
            if len(df_chunk.columns) != len(columns):
                logger.warning(f"Column mismatch in chunk {start_row}-{end_row}")
                return 0
            
            df_chunk.columns = columns
            
            # Load to database using SQLAlchemy
            with self.engine.connect() as conn:
                df_chunk.to_sql(
                    table_name,
                    conn,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi',  # Use multi-row insert
                    chunksize=1000  # Internal chunk size for SQLAlchemy
                )
            
            rows_loaded = len(df_chunk)
            logger.info(f"Loaded {rows_loaded} rows from chunk {start_row}-{end_row}")
            
            # Clear memory
            del df_chunk
            gc.collect()
            
            return rows_loaded
            
        except Exception as e:
            logger.error(f"Error loading chunk {start_row}-{end_row}: {e}")
            return 0
    
    def get_csv_row_count(self, csv_path: str) -> int:
        """Get total number of rows in CSV file"""
        try:
            # Use pandas to count rows efficiently
            df_info = pd.read_csv(csv_path, nrows=0)
            with open(csv_path, 'r', encoding='utf-8') as f:
                # Count lines minus header
                return sum(1 for _ in f) - 1
        except Exception as e:
            logger.error(f"Error counting CSV rows: {e}")
            return 0
    
    def load_csv_parallel(self, csv_path: str, schema: str, table_name: str, 
                         columns: List[str]) -> int:
        """
        Load CSV file in parallel batches
        
        Args:
            csv_path: Path to CSV file
            schema: Database schema
            table_name: Table name
            columns: List of column names
            
        Returns:
            Total number of rows loaded
        """
        total_rows = self.get_csv_row_count(csv_path)
        if total_rows == 0:
            logger.error("CSV file is empty or could not be read")
            return 0
        
        logger.info(f"Loading {total_rows} rows from {csv_path}")
        
        # Create batches
        batches = []
        for start in range(0, total_rows, self.batch_size):
            end = min(start + self.batch_size, total_rows)
            batches.append((start, end))
        
        logger.info(f"Created {len(batches)} batches of size {self.batch_size}")
        
        total_loaded = 0
        start_time = time.time()
        
        # Use ThreadPoolExecutor for parallel loading
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit batch loading tasks
            future_to_batch = {
                executor.submit(
                    self.load_csv_batch, 
                    csv_path, 
                    schema, 
                    table_name, 
                    columns, 
                    start, 
                    end
                ): (start, end) for start, end in batches
            }
            
            # Collect results
            for future in future_to_batch:
                try:
                    rows_loaded = future.result()
                    total_loaded += rows_loaded
                    
                    batch_info = future_to_batch[future]
                    logger.info(f"Completed batch {batch_info[0]}-{batch_info[1]}: {rows_loaded} rows")
                    
                except Exception as e:
                    logger.error(f"Error in batch: {e}")
        
        loading_time = time.time() - start_time
        logger.info(f"Loaded {total_loaded} rows in {loading_time:.2f} seconds")
        logger.info(f"Average speed: {total_loaded/loading_time:.0f} rows/second")
        
        return total_loaded
    
    def fast_copy_load(self, csv_path: str, schema: str, table_name: str, 
                      columns: List[str]) -> int:
        """
        Fast load using PostgreSQL COPY command (single-threaded but very fast)
        
        Args:
            csv_path: Path to CSV file
            schema: Database schema
            table_name: Table name
            columns: List of column names
            
        Returns:
            Number of rows loaded
        """
        try:
            start_time = time.time()
            
            # Use psycopg2 for COPY command
            conn = psycopg2.connect(
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                host=self.db_config['host'],
                port=self.db_config['port']
            )
            
            cur = conn.cursor()
            
            # Get row count before loading
            with open(csv_path, 'r', encoding='utf-8') as f:
                pre_count = sum(1 for _ in f) - 1  # Exclude header
            
            # Perform COPY
            with open(csv_path, 'r', encoding='utf-8') as f:
                cur.copy_expert(
                    f"COPY {schema}.{table_name} ({','.join(columns)}) FROM STDIN WITH CSV HEADER",
                    f
                )
            
            conn.commit()
            cur.close()
            conn.close()
            
            loading_time = time.time() - start_time
            logger.info(f"COPY loaded {pre_count} rows in {loading_time:.2f} seconds")
            logger.info(f"COPY speed: {pre_count/loading_time:.0f} rows/second")
            
            return pre_count
            
        except Exception as e:
            logger.error(f"Error in fast COPY load: {e}")
            return 0
    
    def load_data(self, csv_path: str, schema: str, table_name: str, 
                 columns: List[str], use_copy: bool = True) -> int:
        """
        Main method to load data with performance optimization
        
        Args:
            csv_path: Path to CSV file
            schema: Database schema
            table_name: Table name
            columns: List of column names
            use_copy: Whether to use fast COPY method
            
        Returns:
            Number of rows loaded
        """
        logger.info(f"Starting data load for {csv_path}")
        
        # Create engine if not exists
        if self.engine is None:
            self.create_engine()
        
        # Choose loading method
        if use_copy and os.path.getsize(csv_path) < 100 * 1024 * 1024:  # < 100MB
            # Use fast COPY for smaller files
            rows_loaded = self.fast_copy_load(csv_path, schema, table_name, columns)
        else:
            # Use parallel loading for larger files
            rows_loaded = self.load_csv_parallel(csv_path, schema, table_name, columns)
        
        # Optimize table after loading
        if rows_loaded > 0:
            self.optimize_table(schema, table_name)
        
        return rows_loaded

def main():
    """Main function to run optimized data loading"""
    SCHEMA = "raw_data"
    
    # File paths
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
    
    # Common Crawl
    CC_TABLE = "common_crawl_companies"
    CC_INPUT = os.path.join(PROJECT_ROOT, "data", "common_crawl", "cleaned", "companies_cleaned.csv")
    CC_COLUMNS = ["website_url", "company_name", "industry"]
    
    # ABR
    ABR_TABLE = "abr_companies"
    ABR_INPUT = os.path.join(PROJECT_ROOT, "data", "abr", "cleaned", "companies_cleaned.csv")
    ABR_COLUMNS = ["abn", "entity_name", "entity_type", "entity_status", 
                   "address_line1", "address_line2", "suburb", "state", "postcode", "start_date"]
    
    # Initialize loader
    loader = OptimizedDataLoader(
        batch_size=5000,  # Larger batches for better performance
        max_workers=4     # Conservative worker count
    )
    
    # Load Common Crawl data
    logger.info("Loading Common Crawl data...")
    cc_rows = loader.load_data(CC_INPUT, SCHEMA, CC_TABLE, CC_COLUMNS)
    logger.info(f"Loaded {cc_rows} Common Crawl records")
    
    # Load ABR data
    logger.info("Loading ABR data...")
    abr_rows = loader.load_data(ABR_INPUT, SCHEMA, ABR_TABLE, ABR_COLUMNS)
    logger.info(f"Loaded {abr_rows} ABR records")
    
    logger.info("Data loading completed successfully!")

if __name__ == "__main__":
    main() 