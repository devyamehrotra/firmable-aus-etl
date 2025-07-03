import os
import sys
import pandas as pd

# Get the directory where this script is located

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up two levels to the project root
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
# Build robust paths
input_path = os.path.join(PROJECT_ROOT, 'data', 'abr','raw' ,'companies.csv')
output_path = os.path.join(PROJECT_ROOT, 'data', 'abr', 'cleaned', 'companies_cleaned.csv')
# Ensure output directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Read CSV
df = pd.read_csv(input_path, dtype=str)

# Standardize column names (lowercase, replace spaces with underscores)
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

# Trim whitespace from all string columns
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Validate ABN: keep only rows with 11-digit numeric ABN
if "abn" in df.columns:
    df = df[df["abn"].str.match(r"^\d{11}$", na=False)]

# Validate postcode: keep only 4-digit numeric postcodes
if "postcode" in df.columns:
    df = df[df["postcode"].astype(str).str.match(r"^\d{4}$", na=False)]

# Remove duplicates
df = df.drop_duplicates()
df = df.drop_duplicates(subset=["abn"])

# Filter for active entities (if 'status' column exists)
if "status" in df.columns:
    df = df[df["status"].str.lower() == "active"]

# Ensure output matches schema columns
abr_cols = [
    'abn', 'entity_name', 'entity_type', 'entity_status',
    'address_line1', 'address_line2', 'suburb', 'state', 'postcode', 'start_date'
]
for col in abr_cols:
    if col not in df.columns:
        df[col] = ''
if 'id' in df.columns:
    df = df.drop(columns=['id'])
df = df[abr_cols]

# Save cleaned CSV
df.to_csv(output_path, index=False)

print(f"Cleaned data saved to: {output_path}") 