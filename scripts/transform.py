import pandas as pd
import re
import unidecode
from difflib import SequenceMatcher

def transform_spotify(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['duration_ms'] > 0].copy()
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    df['artist_list'] = df['artists'].apply(aggressive_normalize)
    df['album_name'] = df['album_name'].apply(normalize_text)
    df['track_name'] = df['track_name'].apply(normalize_text)
    df['genre_list'] = df['track_genre'].apply(normalize_genres)

    df = (
        df.groupby('track_id', as_index=False)
        .agg({
            'artist_list': 'first',
            'album_name': 'first',
            'track_name': 'first',
            'popularity': 'first',
            'duration_ms': 'first',
            'explicit': 'first',
            'danceability': 'first',
            'energy': 'first',
            'key': 'first',
            'loudness': 'first',
            'mode': 'first',
            'speechiness': 'first',
            'acousticness': 'first',
            'instrumentalness': 'first',
            'liveness': 'first',
            'valence': 'first',
            'tempo': 'first',
            'time_signature': 'first',
            'genre_list': lambda x: sorted(set([g for sub in x for g in sub]))
        })
    )

    return df

def transform_grammy(df: pd.DataFrame) -> pd.DataFrame:
    if 'img' in df.columns:
        df = df.drop(columns=['img'])

    for col in ['title', 'category', 'nominee', 'workers']:
        df[col] = df[col].apply(normalize_text)
    for col in ['published_at', 'updated_at']:
        df[col] = df[col].apply(normalize_date)
    df['artist'] = df['artist'].apply(normalize_text)

    df['workers'] = impute_column(df, target_col='workers', key_col='artist')

    df = normalize_categories(df, column="category")

    artist_ref = (
        df[df['artist'].notna()]
        .drop_duplicates('nominee')
        .set_index('nominee')['artist']
        .to_dict()
    )
    artist_keywords = ['song', 'artist', 'band']

    def artist_rule(row):
        if pd.notna(row['nominee']) and row['nominee'] in artist_ref:
            return artist_ref[row['nominee']]
        if pd.notna(row['category']) and any(k in row['category'].lower() for k in artist_keywords):
            if pd.notna(row['nominee']):
                return row['nominee']
            if pd.notna(row['workers']):
                return row['workers'].split(",")[0].strip()
        return None

    df['artist'] = impute_column(df, target_col='artist', rule_func=artist_rule)
    df['artist_list'] = df['artist'].apply(aggressive_normalize)

    return df



def normalize_text(value, lowercase=True, keep_basic_symbols=True):
    if pd.isna(value):
        return None
    value = str(value)
    if lowercase:
        value = value.lower()
    value = unidecode.unidecode(value)
    if keep_basic_symbols:
        value = re.sub(r"[^a-z0-9\s'\-]", "", value)
    else:
        value = re.sub(r"[^a-z0-9\s]", "", value)
    return " ".join(value.split())


def normalize_date(value):
    if pd.isna(value):
        return None
    value = pd.to_datetime(value, errors="coerce")
    if pd.isna(value):
        return None
    return value.strftime("%Y-%m-%d %H:%M:%S")


def normalize_genres(value):
    if pd.isna(value):
        return []
    parts = re.split(r'\s*(?:,|/|&|and|y)\s*', str(value).lower())
    return sorted(set([normalize_text(p) for p in parts if p.strip()]))


def impute_column(df, target_col, key_col=None, rule_func=None):
    df = df.copy()
    def impute_row(row):
        if pd.notna(row[target_col]):
            return row[target_col]
        if rule_func is not None:
            candidate = rule_func(row)
            if pd.notna(candidate):
                return candidate
        if key_col is not None and pd.notna(row[key_col]):
            match = df[(df[key_col] == row[key_col]) & (df[target_col].notna())]
            if not match.empty:
                return match.iloc[0][target_col]
        return row[target_col]
    df[target_col] = df.apply(impute_row, axis=1)
    return df[target_col]


def clean_category(value: str) -> str:
    if pd.isna(value):
        return None
    value = str(value).title()
    value = re.sub(r"[-/&().]", "", value)
    return " ".join(value.split()).strip()


def unify_similar_categories(series: pd.Series, threshold: float = 0.85) -> pd.Series:
    cats = series.dropna().unique()
    mapping = {}
    for i in range(len(cats)):
        for j in range(i + 1, len(cats)):
            if SequenceMatcher(None, cats[i], cats[j]).ratio() > threshold:
                mapping[cats[j]] = cats[i]
    return series.replace(mapping)


def normalize_categories(df: pd.DataFrame, column: str = "category") -> pd.DataFrame:
    df[column] = df[column].apply(clean_category)
    df[column] = unify_similar_categories(df[column])
    return df


def aggressive_normalize(name):
    if pd.isna(name):
        return []
    name = unidecode.unidecode(str(name).lower())
    parts = re.split(r'\s*(feat\.|featuring|&|;|,|/|with)\s*', name)
    cleaned = []
    for p in parts:
        p = re.sub(r'\(.*?\)', '', p)
        p = re.sub(r'[^a-z\s]', '', p)
        p = re.sub(r'\b(remix|live|version|edit)\b', '', p)
        p = p.strip()
        if p:
            cleaned.append(p)
    return sorted(set(cleaned))