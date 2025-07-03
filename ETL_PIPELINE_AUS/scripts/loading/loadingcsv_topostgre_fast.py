import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def fast_load_csv_to_postgres(csv_path, table_name, schema, db_user, db_pass, db_host, db_port, db_name, columns):
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    cur = conn.cursor()
    with open(csv_path, 'r', encoding='utf-8') as f:
        cur.copy_expert(
            f"COPY {schema}.{table_name} ({columns}) FROM STDIN WITH CSV HEADER", f
        )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {csv_path} into {schema}.{table_name} (fast COPY method)")

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

fast_load_csv_to_postgres(
    csv_path=CC_INPUT,
    table_name=CC_TABLE,
    schema=SCHEMA,
    db_user=DB_USER,
    db_pass=DB_PASS,
    db_host=DB_HOST,
    db_port=DB_PORT,
    db_name=DB_NAME,
    columns=CC_COLUMNS
)

# ABR
ABR_TABLE = "abr_companies"
ABR_INPUT = os.path.join(PROJECT_ROOT, "data", "abr", "cleaned", "companies_cleaned.csv")
ABR_COLUMNS = "abn, entity_name, entity_type, entity_status, address_line1, address_line2, suburb, state, postcode, start_date"

fast_load_csv_to_postgres(
    csv_path=ABR_INPUT,
    table_name=ABR_TABLE,
    schema=SCHEMA,
    db_user=DB_USER,
    db_pass=DB_PASS,
    db_host=DB_HOST,
    db_port=DB_PORT,
    db_name=DB_NAME,
    columns=ABR_COLUMNS
)