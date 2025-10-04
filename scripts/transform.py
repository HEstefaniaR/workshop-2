import pandas as pd
import unidecode
import re

def transform_spotify(df: pd.DataFrame) -> dict:
    df = df[df['duration_ms'] > 0].copy()
    df['artist_list'] = df['artists'].apply(aggressive_normalize)
    df['album_name'] = df['album_name'].apply(normalize_text)
    df['track_name'] = df['track_name'].apply(normalize_text)
    df['genre_list'] = df['track_genre'].apply(normalize_genres)
    
    df = df.groupby('track_id', as_index=False).agg({
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
    
    dims = build_spotify_dimensions(df)
    return {
        "track_dim": dims["track_dim"],
        "artist_dim": dims["artist_dim"],
        "artist_track_bridge": dims["track_artist_dim"],
        "genre_dim": dims["genre_dim"],
        "genre_track_bridge": dims["genre_song_dim"]
    }


def transform_grammy(df: pd.DataFrame) -> dict:
    df = df.drop(columns=['img', 'workers', 'winner'], errors='ignore')
    df = df[df["nominee"].notna()].copy()
    for col in ['title', 'category', 'nominee']:
        df[col] = df[col].apply(normalize_text)
    for col in ['published_at', 'updated_at']:
        df[col] = df[col].apply(normalize_date)
    df['artist'] = df['artist'].apply(normalize_text)
    
    dims = build_grammy_dimensions(df)
    
    award_fact_df = dims["provisional_nominee_dim"].rename(columns={"nominee_id": "award_fact_id"})
    
    return {
        "grammy_event_dim": dims["grammy_event_dim"],
        "award_fact": award_fact_df
    }


# Limpieza
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
    value = " ".join(value.split())
    return value

def normalize_date(value):
    if pd.isna(value):
        return None
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return None

def normalize_list(value):
    if pd.isna(value):
        return []
    parts = re.split(r'\s*(?:,|and|y|&)\s*', str(value))
    return [p.strip() for p in parts if p.strip()]

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

def normalize_genres(value):
    if pd.isna(value):
        return []
    parts = re.split(r'\s*(?:,|/|&|and|y)\s*', str(value).lower())
    return sorted(set([normalize_text(p) for p in parts if p.strip()]))

# Dimensiones
def build_track_dim(df: pd.DataFrame) -> pd.DataFrame:
    track_dim = df[[
        'track_id', 'track_name', 'album_name', 'popularity', 'duration_ms',
        'explicit', 'danceability', 'energy', 'key',
        'loudness', 'mode', 'speechiness', 'instrumentalness',
        'liveness', 'valence', 'tempo', 'time_signature'
    ]].copy()
    
    track_dim = track_dim.drop_duplicates(subset=[
        'track_name', 'album_name', 'popularity', 'duration_ms', 'explicit', 
        'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 
        'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'
    ])
    track_dim = track_dim.reset_index(drop=True)
    return track_dim


def build_artist_dims(df: pd.DataFrame):
    artist_expanded = df[['track_id', 'artist_list']].explode('artist_list').dropna().drop_duplicates()
    unique_artists = artist_expanded['artist_list'].unique()
    artist_dim = pd.DataFrame({'artist_id': range(1, len(unique_artists) + 1), 'name': unique_artists})
    artist_map = dict(zip(artist_dim['name'], artist_dim['artist_id']))
    track_artist_dim = artist_expanded.assign(artist_id=artist_expanded['artist_list'].map(artist_map)).drop(columns=['artist_list'])
    track_artist_dim.insert(0, "artist_track_id", range(1, len(track_artist_dim) + 1))
    return artist_dim, track_artist_dim

def build_genre_dims(df: pd.DataFrame):
    genre_expanded = df[['track_id', 'genre_list']].explode('genre_list').dropna().drop_duplicates()
    unique_genres = genre_expanded['genre_list'].unique()
    genre_dim = pd.DataFrame({'genre_id': range(1, len(unique_genres) + 1), 'name': unique_genres})
    genre_map = dict(zip(genre_dim['name'], genre_dim['genre_id']))
    genre_song_dim = genre_expanded.assign(genre_id=genre_expanded['genre_list'].map(genre_map)).drop(columns=['genre_list'])
    genre_song_dim.insert(0, "genre_song_id", range(1, len(genre_song_dim) + 1))
    return genre_dim, genre_song_dim

def build_spotify_dimensions(df: pd.DataFrame):
    track_dim = build_track_dim(df)
    artist_dim, track_artist_dim = build_artist_dims(df)
    genre_dim, genre_song_dim = build_genre_dims(df)
    return {
        "track_dim": track_dim,
        "artist_dim": artist_dim,
        "track_artist_dim": track_artist_dim,
        "genre_dim": genre_dim,
        "genre_song_dim": genre_song_dim
    }

def build_grammy_event_dim(df: pd.DataFrame) -> pd.DataFrame:
    grammy_event_dim = df[['year', 'title', 'category', 'published_at', 'updated_at']].drop_duplicates().reset_index(drop=True)
    grammy_event_dim.insert(0, "grammy_event_id", range(1, len(grammy_event_dim) + 1))
    return grammy_event_dim

def build_provisional_nominee(df: pd.DataFrame, grammy_event_dim: pd.DataFrame) -> pd.DataFrame:
    nominee_dim = (
        df[['nominee', 'year', 'title', 'category', 'published_at', 'updated_at']]
        .dropna(subset=['nominee'])
        .drop_duplicates()
        .rename(columns={'nominee': 'name'})
    )

    nominee_dim = nominee_dim.merge(
        grammy_event_dim,
        on=['year', 'title', 'category', 'published_at', 'updated_at'],
        how='left'
    )

    nominee_dim = nominee_dim[['name', 'category', 'grammy_event_id']]
    nominee_dim.insert(0, "nominee_id", range(1, len(nominee_dim) + 1))

    return nominee_dim

def build_grammy_dimensions(df: pd.DataFrame):
    grammy_event_dim = build_grammy_event_dim(df)
    nominee_dim = build_provisional_nominee(df, grammy_event_dim)
    return {
        "grammy_event_dim": grammy_event_dim,
        "provisional_nominee_dim": nominee_dim
    }