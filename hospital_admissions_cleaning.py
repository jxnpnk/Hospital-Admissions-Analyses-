import pandas as pd
import re
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sparse_dot_topn import sp_matmul_topn          # v1.x current API
import networkx as nx
from collections import Counter

# ── 1. Load ──────────────────────────────────────────────────────────────────

df = pd.read_csv(
    "/Users/jeanngugi/Desktop/Personal Projects/Hospital_admissions_Project/healthcare_dataset.csv"
)

print(df.head()) # First 5 rows 
print(df.info()) # Data types and nulls 
df['Name']= df['Name'].str.title() 
print(df.head())

df['Hospital'] = df['Hospital'].str.rstrip(',')

STOPWORDS = {
    'and', 'ltd', 'inc', 'llc', 'plc', 'sons', 'group',
    'the', 'of', 'a', 'an', 'co', 'corp', 'company',
    'associates', 'partners', 'services'
}

def extract_anchor(name: str) -> str:
    """Return the single most distinctive token in the hospital name."""
    name   = str(name).lower()
    tokens = re.sub(r"[^a-z\s]", " ", name).split()
    tokens = [t for t in tokens if t not in STOPWORDS and len(t) >= 4]
    if not tokens:
        return str(name)
    # Use the rarest token as anchor — most distinctive, least generic
    return tokens[0]   # or use frequency-based selection below

# Frequency-weighted anchor: pick the token that is LEAST common globally
# (rare tokens = more distinctive hospital surnames)
all_tokens = []
df['tokens'] = df['Hospital'].apply(
    lambda n: [
        t for t in re.sub(r"[^a-z\s]", " ", str(n).lower()).split()
        if t not in STOPWORDS and len(t) >= 3
    ]
)
for toks in df['tokens']:
    all_tokens.extend(toks)

token_freq = Counter(all_tokens)

def best_anchor(tokens: list) -> str:
    if not tokens:
        return "unknown"
    # Least frequent token = most unique identifier
    return min(tokens, key=lambda t: token_freq[t])

df['Hospital_anchor']   = df['tokens'].apply(best_anchor)
df['Hospital_clean']    = df['Hospital'].apply(
    lambda n: ' '.join(sorted(set(
        t for t in re.sub(r"[^a-z\s]", " ", str(n).lower()).split()
        if t not in STOPWORDS and len(t) >= 3
    )))
)

# Group by anchor surname
anchor_to_gid = {a: i for i, a in enumerate(df['Hospital_anchor'].unique())}
df['Hospital_group']    = df['Hospital_anchor'].map(anchor_to_gid)
df['Hospital_canonical'] = df.groupby('Hospital_anchor')['Hospital'] \
                             .transform(lambda x: x.mode().iloc[0])

# Audit
print(f"Unique anchor groups : {df['Hospital_group'].nunique():,}")
print(f"Avg rows per group   : {len(df) / df['Hospital_group'].nunique():.1f}")
print("\nSample groups:")
print(
    df.groupby('Hospital_anchor')['Hospital']
      .apply(lambda x: sorted(x.unique())[:5])
      .reset_index()
      .head(15)
      .to_string()
)

df.drop(columns=['tokens','Hospital_clean', 'Hospital_anchor', 'Hospital_group', 'Hospital'], inplace=True)
df.to_csv("hospital_admissions_cleaned_1.csv", index=False, sep=";")