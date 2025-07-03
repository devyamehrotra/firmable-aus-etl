import os
import pandas as pd
from rapidfuzz import process, fuzz
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

# --- Robust Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
ABR_CSV = os.path.join(PROJECT_ROOT, 'data', 'abr', 'cleaned', 'companies_cleaned.csv')
CC_CSV = os.path.join(PROJECT_ROOT, 'data', 'common_crawl', 'cleaned', 'companies_cleaned.csv')
DB_URI = "postgresql+psycopg2://newuser:newpassword@localhost:5432/newdb"

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
matches_df = matches_df.drop_duplicates(subset=['company_name', 'abr_company'])

print(f"Total matches found: {len(matches_df)}")

# --- Save to CSV in data/entity_matching ---
entity_matching_dir = os.path.join(PROJECT_ROOT, 'data', 'entity_matching')
os.makedirs(entity_matching_dir, exist_ok=True)
output_csv = os.path.join(entity_matching_dir, 'entity_matches_fuzzy.csv')
matches_df.to_csv(output_csv, index=False)
print(f"Saved matches to CSV: {output_csv}")

# --- Save to PostgreSQL ---
if not matches_df.empty:
    print("Saving matches to PostgreSQL...")
    engine = create_engine(DB_URI)
    with engine.connect() as connection:
        matches_df.to_sql("company_unified", connection, if_exists="append", index=False)
    print(f"Loaded {len(matches_df)} matches to PostgreSQL.")
else:
    print("No matches found.")

print("ETL pipeline complete with fast pandas + rapidfuzz!") 