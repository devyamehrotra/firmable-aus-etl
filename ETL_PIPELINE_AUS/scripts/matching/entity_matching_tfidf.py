import os
import pandas as pd
from rapidfuzz import process, fuzz
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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

# --- Robust Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
ABR_CSV = os.path.join(PROJECT_ROOT, 'data', 'abr', 'cleaned', 'companies_cleaned.csv')
CC_CSV = os.path.join(PROJECT_ROOT, 'data', 'common_crawl', 'cleaned', 'companies_cleaned.csv')

print(f"Loading ABR from: {ABR_CSV}")
abr = pd.read_csv(ABR_CSV)
print(f"Loaded {len(abr)} ABR records.")
print(f"Loading Common Crawl from: {CC_CSV}")
cc = pd.read_csv(CC_CSV)
print(f"Loaded {len(cc)} Common Crawl records.")

def clean_name(name):
    if not isinstance(name, str):
        return ''
    return name.strip().upper()

def create_duplicate_safe_table():
    """Create entity matches table with proper constraints for duplicate handling"""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.entity_matches (
        id SERIAL PRIMARY KEY,
        abn VARCHAR(255),
        url TEXT,
        company_name VARCHAR(500),
        abr_company VARCHAR(500),
        industry VARCHAR(255),
        entity_type VARCHAR(255),
        entity_status VARCHAR(255),
        address TEXT,
        postcode VARCHAR(10),
        state VARCHAR(50),
        start_date DATE,
        match_confidence FLOAT,
        source_commoncrawl_id VARCHAR(255),
        source_abr_id VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        -- Unique constraint to prevent duplicates
        UNIQUE(abn, url, company_name, abr_company)
    );
    """
    cur.execute(create_table_sql)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Created duplicate-safe entity_matches table")

def upsert_matches_to_postgres(matches_df, table_name, schema):
    """Save matches using UPSERT to handle duplicates gracefully"""
    if matches_df.empty:
        print("No matches to save.")
        return
    
    # Create temporary CSV file
    temp_csv = f"temp_matches_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    matches_df.to_csv(temp_csv, index=False)
    
    # Connect to database
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    
    # Create temporary table
    temp_table = "temp_matches_upsert"
    temp_create_sql = f"""
    CREATE TEMP TABLE {temp_table} (
        LIKE {schema}.{table_name} INCLUDING ALL
    ) ON COMMIT DROP;
    """
    cur.execute(temp_create_sql)
    
    # Load data to temp table using COPY
    columns = "abn, url, company_name, abr_company, industry, entity_type, entity_status, address, postcode, state, start_date, match_confidence, source_commoncrawl_id, source_abr_id"
    with open(temp_csv, 'r', encoding='utf-8') as f:
        cur.copy_expert(
            f"COPY {temp_table} ({columns}) FROM STDIN WITH CSV HEADER", f
        )
    
    # Perform UPSERT
    upsert_sql = f"""
    INSERT INTO {schema}.{table_name} ({columns})
    SELECT {columns} FROM {temp_table}
    ON CONFLICT (abn, url, company_name, abr_company) 
    DO UPDATE SET
        industry = EXCLUDED.industry,
        entity_type = EXCLUDED.entity_type,
        entity_status = EXCLUDED.entity_status,
        address = EXCLUDED.address,
        postcode = EXCLUDED.postcode,
        state = EXCLUDED.state,
        start_date = EXCLUDED.start_date,
        match_confidence = EXCLUDED.match_confidence,
        source_commoncrawl_id = EXCLUDED.source_commoncrawl_id,
        source_abr_id = EXCLUDED.source_abr_id,
        updated_at = CURRENT_TIMESTAMP
    WHERE {schema}.{table_name}.match_confidence < EXCLUDED.match_confidence
       OR {schema}.{table_name}.industry IS DISTINCT FROM EXCLUDED.industry
       OR {schema}.{table_name}.entity_type IS DISTINCT FROM EXCLUDED.entity_type
       OR {schema}.{table_name}.entity_status IS DISTINCT FROM EXCLUDED.entity_status;
    """
    
    cur.execute(upsert_sql)
    affected_rows = cur.rowcount
    
    # Get total counts
    cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
    total_processed = cur.fetchone()[0]
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Clean up temporary file
    os.remove(temp_csv)
    
    print(f"✅ UPSERT complete: {total_processed} records processed, {affected_rows} updated")
    print(f"Saved matches to {schema}.{table_name} (duplicate-safe method)")

abr['entity_name_clean'] = abr['entity_name'].apply(clean_name)
cc['company_name_clean'] = cc['company_name'].apply(clean_name)
abr['block'] = abr['entity_name_clean'].str[:2]  # Block by first two letters
cc['block'] = cc['company_name_clean'].str[:2]

# --- Fuzzy matching with blocking and parallelization ---
def match_block(args):
    abr_block, cc_block, block = args
    matches = []
    abr_names = abr_block['entity_name_clean'].tolist()
    print(f"Processing block '{block}': {len(abr_block)} ABR x {len(cc_block)} CC")
    for cc_idx, cc_row in cc_block.iterrows():
        cc_name = cc_row['company_name_clean']
        best_match, score, _ = process.extractOne(cc_name, abr_names, scorer=fuzz.token_sort_ratio)
        if score >= 85:
            matches.append({
                "abn": abr_block.iloc[abr_names.index(best_match)].get("abn"),
                "url": cc_row.get("url"),
                "company_name": cc_row['company_name'],
                "abr_company": abr_block.iloc[abr_names.index(best_match)]['entity_name'],
                "industry": cc_row.get("industry", None),
                "entity_type": abr_block.iloc[abr_names.index(best_match)].get("entity_type", None),
                "entity_status": abr_block.iloc[abr_names.index(best_match)].get("entity_status", None),
                "address": abr_block.iloc[abr_names.index(best_match)].get("address", None),
                "postcode": abr_block.iloc[abr_names.index(best_match)].get("postcode", None),
                "state": abr_block.iloc[abr_names.index(best_match)].get("state", None),
                "start_date": abr_block.iloc[abr_names.index(best_match)].get("start_date", None),
                "match_confidence": score,
                "source_commoncrawl_id": cc_row.get("id", None),
                "source_abr_id": abr_block.iloc[abr_names.index(best_match)].get("id", None)
            })
    print(f"Block '{block}': {len(matches)} matches found.")
    return matches

# Prepare blocks
blocks = sorted(set(abr['block'].dropna().unique()) | set(cc['block'].dropna().unique()))
block_args = []
for block in blocks:
    abr_block = abr[abr['block'] == block]
    cc_block = cc[cc['block'] == block]
    if not abr_block.empty and not cc_block.empty:
        block_args.append((abr_block, cc_block, block))

print(f"Total blocks to process: {len(block_args)}")

# Parallel matching
matches = []
with ThreadPoolExecutor() as executor:
    for result in executor.map(match_block, block_args):
        matches.extend(result)

matches_df = pd.DataFrame(matches)

# Remove exact duplicates before saving
matches_df = matches_df.drop_duplicates(subset=['abn', 'url', 'company_name', 'abr_company'])

print(f"Total matches found: {len(matches_df)}")

# Create the table first
create_duplicate_safe_table()

# --- Save to CSV in data/entity_matching ---
entity_matching_dir = os.path.join(PROJECT_ROOT, 'data', 'entity_matching')
os.makedirs(entity_matching_dir, exist_ok=True)
output_csv = os.path.join(entity_matching_dir, 'entity_matches_fuzzy.csv')
matches_df.to_csv(output_csv, index=False)
print(f"Saved matches to CSV: {output_csv}")

# --- Save to PostgreSQL using duplicate-safe UPSERT method ---
if not matches_df.empty:
    print("Saving matches to PostgreSQL using duplicate-safe method...")
    upsert_matches_to_postgres(matches_df, "entity_matches", SCHEMA)
else:
    print("No matches found.")

print("ETL pipeline complete with fast pandas + rapidfuzz!") 