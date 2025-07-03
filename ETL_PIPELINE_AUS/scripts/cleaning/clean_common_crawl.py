import os
import pandas as pd
import re

def clean_url(url):
    if pd.isna(url):
        return None
    return str(url).strip()[:500]

def clean_company_name(name):
    if pd.isna(name):
        return None
    return re.sub(' +', ' ', name.strip().title())

def main():
    # Get the directory of this script
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    # Go up two levels to the project root
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
    # Build robust paths
    input_path = os.path.join(PROJECT_ROOT, 'data', 'common_crawl','raw' ,'companies.csv')
    output_path = os.path.join(PROJECT_ROOT, 'data', 'common_crawl', 'cleaned', 'companies_cleaned.csv')

    df = pd.read_csv(input_path, dtype=str)
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    df['website_url'] = df['website_url'].apply(clean_url)
    df['company_name'] = df['company_name'].apply(clean_company_name)
    df = df.dropna(subset=['website_url', 'company_name'])
    df = df.drop_duplicates(subset=['website_url', 'company_name'])
    if 'id' in df.columns:
        df = df.drop(columns=['id'])
    df.to_csv(output_path, index=False)
    print(f"Data cleaned and saved to {output_path}")

if __name__ == "__main__":
    main() 