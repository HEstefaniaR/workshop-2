import pandas as pd

def merge_dw(spotify_dims: dict[str, pd.DataFrame], grammy_dims: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    track_dim = spotify_dims["track_dim"]
    artist_dim = spotify_dims["artist_dim"]
    artist_track_bridge = spotify_dims["artist_track_bridge"]
    genre_dim = spotify_dims["genre_dim"]
    genre_track_bridge = spotify_dims["genre_track_bridge"]

    grammy_event_dim = grammy_dims["grammy_event_dim"]
    award_fact_df = grammy_dims["award_fact"].copy()

    # Asegurar award_fact_id
    if 'award_fact_id' not in award_fact_df.columns:
        award_fact_df.insert(0, 'award_fact_id', range(1, len(award_fact_df) + 1))

    # Normalizar columnas
    award_fact_df['name'] = award_fact_df['name'].replace(['nan', ''], pd.NA)
    if 'category' not in award_fact_df.columns:
        award_fact_df['category'] = 'unknown'
    award_fact_df['category'] = award_fact_df['category'].apply(classify_category)

    # Construir candidatos
    artist_matches = (
        award_fact_df[award_fact_df['category'].isin(['artist', 'unknown'])]
        .merge(artist_dim, left_on='name', right_on='name', how='inner')
        .drop_duplicates(subset=['award_fact_id', 'artist_id'])
    )
    artist_matches["match_type"] = "artist"

    track_matches = (
        award_fact_df[award_fact_df['category'].isin(['track', 'unknown'])]
        .merge(track_dim, left_on='name', right_on='track_name', how='inner')
        .drop_duplicates(subset=['award_fact_id', 'track_id'])
    )
    track_matches["match_type"] = "track"

    # Resolver conflictos award_fact_id presentes en ambos
    both = set(artist_matches['award_fact_id']) & set(track_matches['award_fact_id'])
    for aid in both:
        cat_type = award_fact_df.loc[award_fact_df['award_fact_id'] == aid, 'category'].iloc[0]
        if cat_type == 'artist':
            track_matches = track_matches[track_matches['award_fact_id'] != aid]
        elif cat_type == 'track':
            artist_matches = artist_matches[artist_matches['award_fact_id'] != aid]

    # Construir award_fact
    artist_awards = artist_matches.rename(columns={'artist_id': 'winner_artist_id'})
    artist_awards['winner_track_id'] = None
    artist_awards = artist_awards.merge(
        grammy_event_dim[['grammy_event_id', 'award_fact_id']],
        on='award_fact_id',
        how='left'
    )
    artist_awards = artist_awards[['winner_artist_id', 'winner_track_id', 'grammy_event_id']]

    track_awards = track_matches.rename(columns={'track_id': 'winner_track_id'})
    track_awards['winner_artist_id'] = None
    track_awards = track_awards.merge(
        grammy_event_dim[['grammy_event_id', 'award_fact_id']],
        on='award_fact_id',
        how='left'
    )
    track_awards = track_awards[['winner_artist_id', 'winner_track_id', 'grammy_event_id']]

    award_fact = pd.concat([artist_awards, track_awards], ignore_index=True)

    # Validaciones
    valid_event_ids = set(grammy_event_dim['grammy_event_id'])
    award_fact = award_fact[award_fact['grammy_event_id'].isin(valid_event_ids)]
    award_fact = award_fact.drop_duplicates(subset=['winner_artist_id', 'winner_track_id', 'grammy_event_id'])

    # Asignar ID final
    award_fact.insert(0, 'award_fact_id', range(1, len(award_fact) + 1))
    award_fact['winner_artist_id'] = award_fact['winner_artist_id'].astype('Int64')
    award_fact['winner_track_id'] = award_fact['winner_track_id'].astype('string')

    print("=== Award Fact Integridad ===")
    print(f"Premios totales: {len(award_fact)}")
    print(f"Premios solo artista: {award_fact['winner_artist_id'].notna().sum()}")
    print(f"Premios solo track: {award_fact['winner_track_id'].notna().sum()}")
    print("=============================")

    return {
        "track_dim": track_dim,
        "artist_dim": artist_dim,
        "artist_track_bridge": artist_track_bridge,
        "genre_dim": genre_dim,
        "genre_track_bridge": genre_track_bridge,
        "grammy_event_dim": grammy_event_dim,
        "award_fact": award_fact
    }

def classify_category(cat: str) -> str:
    if pd.isna(cat):
        return 'track'
    cat = str(cat).lower()
    if any(w in cat for w in ['song', 'record', 'track']):
        return 'track'
    if any(w in cat for w in ['artist', 'album', 'group', 'duo']):
        return 'artist'
    return 'track'

def build_award_candidates(award_fact_df: pd.DataFrame, artist_dim: pd.DataFrame, track_dim: pd.DataFrame):
    def classify_category(category: str) -> str:
        if pd.isna(category):
            return "unknown"
        cat = str(category).lower()
        if any(word in cat for word in ["song", "record", "track"]):
            return "track"
        if any(word in cat for word in ["artist", "album", "group", "duo"]):
            return "artist"
        return "unknown"
    
    award_fact_df["category"] = award_fact_df["category"].apply(classify_category)
    
    artist_matches = (
        award_fact_df[award_fact_df["category"].isin(["artist", "unknown"])]
        .merge(
            artist_dim,
            left_on="name",
            right_on="name",
            how="left",
            indicator=True
        )
        .query("_merge == 'both'")
        .drop_duplicates(subset=["award_fact_id", "artist_id"])
    )
    artist_matches["match_type"] = "artist"

    track_matches = (
        award_fact_df[award_fact_df["category"].isin(["track", "unknown"])]
        .merge(
            track_dim,
            left_on="name",
            right_on="track_name",
            how="left",
            indicator=True
        )
        .query("_merge == 'both'")
        .drop_duplicates(subset=["award_fact_id", "track_id"])
    )
    track_matches["match_type"] = "track"
    
    both = set(artist_matches["award_fact_id"]) & set(track_matches["award_fact_id"])
    for award_id in both:
        cat_type = award_fact_df.loc[award_fact_df["award_fact_id"] == award_id, "category"].iloc[0]
        if cat_type == "artist":
            track_matches = track_matches[track_matches["award_fact_id"] != award_id]
        elif cat_type == "track":
            artist_matches = artist_matches[artist_matches["award_fact_id"] != award_id]
    
    print("Coincidencias encontradas:")
    print(f"Artistas: {len(artist_matches)}")
    print(f"Tracks: {len(track_matches)}")
    
    return artist_matches, track_matches


def build_award_fact(artist_matches: pd.DataFrame, track_matches: pd.DataFrame, grammy_event_dim: pd.DataFrame) -> pd.DataFrame:
    artist_awards = artist_matches[['artist_id', 'grammy_event_id']].copy()
    artist_awards['winner_track_id'] = None
    artist_awards.rename(columns={'artist_id': 'winner_artist_id'}, inplace=True)
    artist_awards = artist_awards[['winner_artist_id', 'winner_track_id', 'grammy_event_id']]
    
    track_awards = track_matches[['track_id', 'grammy_event_id']].copy()
    track_awards['winner_artist_id'] = None
    track_awards.rename(columns={'track_id': 'winner_track_id'}, inplace=True)
    track_awards = track_awards[['winner_artist_id', 'winner_track_id', 'grammy_event_id']]
    
    award_fact = pd.concat([artist_awards, track_awards], ignore_index=True)
    
    initial_count = len(award_fact)
    award_fact = award_fact[
        (award_fact['winner_artist_id'].notna()) | 
        (award_fact['winner_track_id'].notna())
    ]
    removed_no_winner = initial_count - len(award_fact)
    if removed_no_winner > 0:
        print(f"Eliminados {removed_no_winner} registros sin ganador identificado")
    
    valid_event_ids = set(grammy_event_dim['grammy_event_id'].values)
    initial_count = len(award_fact)
    award_fact = award_fact[award_fact['grammy_event_id'].isin(valid_event_ids)]
    removed_invalid_events = initial_count - len(award_fact)
    if removed_invalid_events > 0:
        print(f"Eliminados {removed_invalid_events} registros con grammy_event_id inválido")
    
    initial_count = len(award_fact)
    award_fact = award_fact.drop_duplicates(subset=['winner_artist_id', 'winner_track_id', 'grammy_event_id'], keep='first')
    removed_duplicates = initial_count - len(award_fact)
    if removed_duplicates > 0:
        print(f"Eliminados {removed_duplicates} registros duplicados")
    
    award_fact.insert(0, 'award_fact_id', range(1, len(award_fact) + 1))
    
    award_fact['winner_artist_id'] = award_fact['winner_artist_id'].apply(
        lambda x: int(x) if pd.notna(x) else None
    )
    award_fact['winner_track_id'] = award_fact['winner_track_id'].apply(
        lambda x: str(x) if pd.notna(x) and x != 'nan' else None
    )
    
    print(f"Registros finales en award_fact: {len(award_fact)}")
    
    return award_fact