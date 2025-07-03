import os
import pandas as pd
from sqlalchemy import create_engine

# --- CONFIGURATION ---
DB_USER = "postgres"
DB_PASS = "devya12345"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "firmable_db"

# Table and schema names
CC_TABLE = "common_crawl_companies"
ABR_TABLE = "abr_companies"
SCHEMA = "raw_data"

# Robust project-root-relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
CC_INPUT = os.path.join(PROJECT_ROOT, "data", "common_crawl", "cleaned", "companies_cleaned.csv")
ABR_INPUT = os.path.join(PROJECT_ROOT, "data", "abr", "cleaned", "companies_cleaned.csv")

# Create SQLAlchemy engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Load Common Crawl data
print(f"Reading Common Crawl data from: {CC_INPUT}")
df_cc = pd.read_csv(CC_INPUT)
for col in ['website_url', 'company_name', 'industry']:
    if col in df_cc.columns:
        df_cc[col] = df_cc[col].astype(str).str.slice(0, 10000)
df_cc.to_sql(CC_TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

# Load ABR data
print(f"Reading ABR data from: {ABR_INPUT}")
df_abr = pd.read_csv(ABR_INPUT)
df_abr.to_sql(ABR_TABLE, engine, schema=SCHEMA, if_exists="append", index=False)

print("Data loaded successfully!")