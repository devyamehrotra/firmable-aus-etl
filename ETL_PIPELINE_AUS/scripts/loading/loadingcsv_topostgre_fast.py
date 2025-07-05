import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def upsert_common_crawl_to_postgres(csv_path, table_name, schema, db_user, db_pass, db_host, db_port, db_name, columns, unique_columns):
    """Load Common Crawl CSV to PostgreSQL using UPSERT"""
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    cur = conn.cursor()
    
    # Create Common Crawl table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        id SERIAL PRIMARY KEY,
        website_url TEXT,
        company_name VARCHAR(500),
        industry TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE({unique_columns})
    );
    """
    cur.execute(create_table_sql)
    conn.commit()
    
    # Read CSV in chunks
    chunk_size = 10000
    total_inserted = 0
    total_updated = 0
    
    for chunk_num, chunk_df in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
        print(f"Processing chunk {chunk_num + 1}...")
        
        # Create temporary table
        temp_table = f"temp_{table_name}_{chunk_num}"
        temp_create_sql = f"""
        CREATE TEMP TABLE {temp_table} (
            LIKE {schema}.{table_name} INCLUDING ALL
        ) ON COMMIT DROP;
        """
        cur.execute(temp_create_sql)
        
        # Load chunk to temp table
        temp_csv = f"temp_chunk_{chunk_num}.csv"
        chunk_df.to_csv(temp_csv, index=False)
        
        with open(temp_csv, 'r', encoding='utf-8') as f:
            cur.copy_expert(
                f"COPY {temp_table} ({columns}) FROM STDIN WITH CSV HEADER", f
            )
        
        # Perform UPSERT for Common Crawl
        upsert_sql = f"""
        INSERT INTO {schema}.{table_name} ({columns})
        SELECT {columns} FROM {temp_table}
        ON CONFLICT ({unique_columns}) 
        DO UPDATE SET
            website_url = EXCLUDED.website_url,
            company_name = EXCLUDED.company_name,
            industry = EXCLUDED.industry,
            updated_at = CURRENT_TIMESTAMP
        WHERE {schema}.{table_name}.website_url IS DISTINCT FROM EXCLUDED.website_url
           OR {schema}.{table_name}.company_name IS DISTINCT FROM EXCLUDED.company_name
           OR {schema}.{table_name}.industry IS DISTINCT FROM EXCLUDED.industry;
        """
        
        cur.execute(upsert_sql)
        
        # Get affected rows
        affected_rows = cur.rowcount
        total_updated += affected_rows
        
        # Count new inserts
        cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
        chunk_count = cur.fetchone()[0]
        total_inserted += chunk_count - affected_rows
        
        conn.commit()
        
        # Clean up temp file
        os.remove(temp_csv)
        
        print(f"Chunk {chunk_num + 1}: {chunk_count} records processed, {affected_rows} updated")
    
    cur.close()
    conn.close()
    
    print(f"✅ UPSERT complete: {total_inserted} new records, {total_updated} updated records")
    print(f"Loaded {csv_path} into {schema}.{table_name} (UPSERT method)")

def upsert_abr_to_postgres(csv_path, table_name, schema, db_user, db_pass, db_host, db_port, db_name, columns, unique_columns):
    """Load ABR CSV to PostgreSQL using UPSERT"""
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    cur = conn.cursor()
    
    # Create ABR table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        id SERIAL PRIMARY KEY,
        abn VARCHAR(255),
        entity_name TEXT,
        entity_type VARCHAR(255),
        entity_status VARCHAR(255),
        address_line1 TEXT,
        address_line2 TEXT,
        suburb VARCHAR(255),
        state VARCHAR(50),
        postcode VARCHAR(10),
        start_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE({unique_columns})
    );
    """
    cur.execute(create_table_sql)
    conn.commit()
    
    # Read CSV in chunks
    chunk_size = 10000
    total_inserted = 0
    total_updated = 0
    
    for chunk_num, chunk_df in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
        print(f"Processing chunk {chunk_num + 1}...")
        
        # Create temporary table
        temp_table = f"temp_{table_name}_{chunk_num}"
        temp_create_sql = f"""
        CREATE TEMP TABLE {temp_table} (
            LIKE {schema}.{table_name} INCLUDING ALL
        ) ON COMMIT DROP;
        """
        cur.execute(temp_create_sql)
        
        # Load chunk to temp table
        temp_csv = f"temp_chunk_{chunk_num}.csv"
        chunk_df.to_csv(temp_csv, index=False)
        
        with open(temp_csv, 'r', encoding='utf-8') as f:
            cur.copy_expert(
                f"COPY {temp_table} ({columns}) FROM STDIN WITH CSV HEADER", f
            )
        
        # Perform UPSERT for ABR
        upsert_sql = f"""
        INSERT INTO {schema}.{table_name} ({columns})
        SELECT {columns} FROM {temp_table}
        ON CONFLICT ({unique_columns}) 
        DO UPDATE SET
            entity_type = EXCLUDED.entity_type,
            entity_status = EXCLUDED.entity_status,
            address_line1 = EXCLUDED.address_line1,
            address_line2 = EXCLUDED.address_line2,
            suburb = EXCLUDED.suburb,
            state = EXCLUDED.state,
            postcode = EXCLUDED.postcode,
            start_date = EXCLUDED.start_date,
            updated_at = CURRENT_TIMESTAMP
        WHERE {schema}.{table_name}.entity_type IS DISTINCT FROM EXCLUDED.entity_type
           OR {schema}.{table_name}.entity_status IS DISTINCT FROM EXCLUDED.entity_status
           OR {schema}.{table_name}.address_line1 IS DISTINCT FROM EXCLUDED.address_line1
           OR {schema}.{table_name}.suburb IS DISTINCT FROM EXCLUDED.suburb
           OR {schema}.{table_name}.state IS DISTINCT FROM EXCLUDED.state
           OR {schema}.{table_name}.postcode IS DISTINCT FROM EXCLUDED.postcode
           OR {schema}.{table_name}.start_date IS DISTINCT FROM EXCLUDED.start_date;
        """
        
        cur.execute(upsert_sql)
        
        # Get affected rows
        affected_rows = cur.rowcount
        total_updated += affected_rows
        
        # Count new inserts
        cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
        chunk_count = cur.fetchone()[0]
        total_inserted += chunk_count - affected_rows
        
        conn.commit()
        
        # Clean up temp file
        os.remove(temp_csv)
        
        print(f"Chunk {chunk_num + 1}: {chunk_count} records processed, {affected_rows} updated")
    
    cur.close()
    conn.close()
    
    print(f"✅ UPSERT complete: {total_inserted} new records, {total_updated} updated records")
    print(f"Loaded {csv_path} into {schema}.{table_name} (UPSERT method)")

# --- CONFIGURATION FROM ENVIRONMENT VARIABLES ---
DB_USER = os.getenv('DB_USER', 'newuser')
DB_PASS = os.getenv('DB_PASSWORD', 'newpassword')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'newdb')
SCHEMA = "raw_data"

# Validate required environment variables
if not DB_PASS:
    raise ValueError("DB_PASSWORD environment variable is required")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))

# Common Crawl
CC_TABLE = "common_crawl_companies"
CC_INPUT = os.path.join(PROJECT_ROOT, "data", "common_crawl", "cleaned", "companies_cleaned.csv")
CC_COLUMNS = "website_url, company_name, industry"
CC_UNIQUE = "website_url, company_name"  # Unique constraint

upsert_common_crawl_to_postgres(
    csv_path=CC_INPUT,
    table_name=CC_TABLE,
    schema=SCHEMA,
    db_user=DB_USER,
    db_pass=DB_PASS,
    db_host=DB_HOST,
    db_port=DB_PORT,
    db_name=DB_NAME,
    columns=CC_COLUMNS,
    unique_columns=CC_UNIQUE
)

# ABR
ABR_TABLE = "abr_companies"
ABR_INPUT = os.path.join(PROJECT_ROOT, "data", "abr", "cleaned", "companies_cleaned.csv")
ABR_COLUMNS = "abn, entity_name, entity_type, entity_status, address_line1, address_line2, suburb, state, postcode, start_date"
ABR_UNIQUE = "abn"  # ABN should be unique

upsert_abr_to_postgres(
    csv_path=ABR_INPUT,
    table_name=ABR_TABLE,
    schema=SCHEMA,
    db_user=DB_USER,
    db_pass=DB_PASS,
    db_host=DB_HOST,
    db_port=DB_PORT,
    db_name=DB_NAME,
    columns=ABR_COLUMNS,
    unique_columns=ABR_UNIQUE
)