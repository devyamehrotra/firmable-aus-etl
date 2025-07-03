import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, trim, col
from rapidfuzz import process, fuzz
import pandas as pd
from sqlalchemy import create_engine

# --- Robust Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
ABR_CSV = os.path.join(PROJECT_ROOT, 'data', 'abr', 'cleaned', 'companies_cleaned.csv')
CC_CSV = os.path.join(PROJECT_ROOT, 'data', 'common_crawl', 'cleaned', 'companies_cleaned.csv')
DB_URI = "postgresql+psycopg2://postgres:devya12345@localhost:5432/firmable_db"

print(f"[Spark] Loading ABR from: {ABR_CSV}")
spark = SparkSession.builder.appName("Firmable ETL Spark").getOrCreate()
abr = spark.read.csv(ABR_CSV, header=True, inferSchema=True)
print(f"[Spark] Loaded {abr.count()} ABR records.")
print(f"[Spark] Loading Common Crawl from: {CC_CSV}")
cc = spark.read.csv(CC_CSV, header=True, inferSchema=True)
print(f"[Spark] Loaded {cc.count()} Common Crawl records.")

# Clean and normalize
abr = abr.withColumn("entity_name", upper(trim(col("entity_name"))))
cc = cc.withColumn("company_name", upper(trim(col("company_name"))))

# Collect to driver for matching (for demo; for huge data, use distributed join/blocking)
abr_pd = abr.limit(1000).toPandas()  # Sample for demo
cc_pd = cc.toPandas()

print(f"[Spark] Matching {len(abr_pd)} ABR sample vs {len(cc_pd)} CC records (fuzzy)...")
matches = []
cc_names = cc_pd["company_name"].tolist()
cc_indices = cc_pd.index.tolist()

for idx, abr_row in abr_pd.iterrows():
    name = abr_row["entity_name"]
    result = process.extractOne(
        name, list(zip(cc_names, cc_indices)), scorer=fuzz.token_sort_ratio
    )
    if result and result[1] >= 70:
        best_match, score, cc_idx = result
        cc_row = cc_pd.loc[cc_idx]
        matches.append({
            "abn": abr_row.get("abn"),
            "url": cc_row.get("url"),
            "company_name": name,
            "industry": cc_row.get("industry", None),
            "entity_type": abr_row.get("entity_type", None),
            "entity_status": abr_row.get("entity_status", None),
            "address": abr_row.get("address", None),
            "postcode": abr_row.get("postcode", None),
            "state": abr_row.get("state", None),
            "start_date": abr_row.get("start_date", None),
            "match_confidence": score,
            "source_commoncrawl_id": cc_row.get("id", None),
            "source_abr_id": abr_row.get("id", None)
        })
    if idx % 100 == 0:
        print(f"[Spark] Processed {idx} ABR records...")

matches_df = pd.DataFrame(matches)

# Save to PostgreSQL
if not matches_df.empty:
    print(f"[Spark] Saving {len(matches_df)} matches to PostgreSQL...")
    engine = create_engine(DB_URI)
    matches_df.to_sql("company_unified", engine, if_exists="append", index=False)
    print(f"[Spark] Loaded {len(matches_df)} matches to PostgreSQL.")
else:
    print("[Spark] No matches found.")

print("[Spark] ETL pipeline complete with PySpark!") 