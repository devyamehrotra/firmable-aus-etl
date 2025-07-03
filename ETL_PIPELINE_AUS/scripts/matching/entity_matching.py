import os
import pandas as pd
import re
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Robust project-root-relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
ABR_INPUT = os.path.join(PROJECT_ROOT, 'data', 'abr', 'cleaned', 'companies_cleaned.csv')
CC_INPUT = os.path.join(PROJECT_ROOT, 'data', 'common_crawl', 'cleaned', 'companies_cleaned.csv')
OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'entity_matches.csv')

# Load your cleaned CSVs
abr = pd.read_csv(ABR_INPUT)
cc = pd.read_csv(CC_INPUT)

def normalize_name(name):
    if pd.isna(name):
        return ""
    return str(name).lower().strip()

abr['norm_name'] = abr['entity_name'].apply(normalize_name)
cc['norm_name'] = cc['company_name'].apply(normalize_name)

# --- Fuzzy Matching ---
matches = []
for idx_cc, row_cc in cc.iterrows():
    best_score = 0
    best_match = None
    for idx_abr, row_abr in abr.iterrows():
        score = fuzz.token_sort_ratio(row_cc['norm_name'], row_abr['norm_name'])
        if score > best_score:
            best_score = score
            best_match = row_abr['entity_name']
    if best_score >= 85:  # Set your own threshold
        matches.append({
            'cc_company': row_cc['company_name'],
            'abr_company': best_match,
            'score': best_score,
            'method': 'fuzzy'
        })

# --- NLP (TF-IDF + Cosine Similarity) ---
abr_names = abr['norm_name'].tolist()
cc_names = cc['norm_name'].tolist()
vectorizer = TfidfVectorizer().fit(abr_names + cc_names)
abr_vecs = vectorizer.transform(abr_names)
cc_vecs = vectorizer.transform(cc_names)

for i, cc_vec in enumerate(cc_vecs):
    sims = cosine_similarity(cc_vec, abr_vecs)[0]
    best_idx = sims.argmax()
    best_score = sims[best_idx]
    if best_score >= 0.7:  # Set your own threshold
        matches.append({
            'cc_company': cc.iloc[i]['company_name'],
            'abr_company': abr.iloc[best_idx]['entity_name'],
            'score': best_score,
            'method': 'tfidf'
        })

# --- Combine and Save ---
matches_df = pd.DataFrame(matches)
matches_df = matches_df.drop_duplicates(subset=['cc_company', 'abr_company'])
matches_df.to_csv(OUTPUT_PATH, index=False)
print(f"Entity matching complete. Results saved to {OUTPUT_PATH}")