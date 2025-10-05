import pandas as pd

def merge_dw(spotify_dims: dict[str, pd.DataFrame], grammy_dims: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    track_dim = spotify_dims["track_dim"]
    artist_dim = spotify_dims["artist_dim"]
    artist_track_bridge = spotify_dims["artist_track_bridge"]
    genre_dim = spotify_dims["genre_dim"]
    genre_track_bridge = spotify_dims["genre_track_bridge"]
    grammy_event_dim = grammy_dims["grammy_event_dim"]
    award_fact_df = grammy_dims["award_fact"]
    
    award_fact_df['name'] = award_fact_df['name'].replace(['nan', ''], pd.NA)
    
    award_fact_df['category_type'] = award_fact_df['category'].apply(classify_category)
    
    artist_matches, track_matches = build_award_candidates(award_fact_df, artist_dim, track_dim)
    award_fact = build_award_fact(artist_matches, track_matches, grammy_event_dim)
    
    used_event_ids = set(award_fact['grammy_event_id'].unique())
    initial_events = len(grammy_event_dim)
    grammy_event_dim = grammy_event_dim[grammy_event_dim['grammy_event_id'].isin(used_event_ids)]
    removed_events = initial_events - len(grammy_event_dim)
    print(f"Grammy events: {removed_events} eliminados, {len(grammy_event_dim)} mantenidos")
    
    used_artist_ids = set(award_fact[award_fact['winner_artist_id'].notna()]['winner_artist_id'].unique())
    initial_artists = len(artist_dim)
    artist_dim = artist_dim[artist_dim['artist_id'].isin(used_artist_ids)]
    removed_artists = initial_artists - len(artist_dim)
    print(f"Artistas: {removed_artists} eliminados, {len(artist_dim)} mantenidos")
    
    used_track_ids = set(award_fact[award_fact['winner_track_id'].notna()]['winner_track_id'].unique())
    initial_tracks = len(track_dim)
    track_dim = track_dim[track_dim['track_id'].isin(used_track_ids)]
    removed_tracks = initial_tracks - len(track_dim)
    print(f"Tracks: {removed_tracks} eliminados, {len(track_dim)} mantenidos")
    
    initial_at_bridge = len(artist_track_bridge)
    artist_track_bridge = artist_track_bridge[
        artist_track_bridge['artist_id'].isin(used_artist_ids) &
        artist_track_bridge['track_id'].isin(used_track_ids)
    ]
    removed_at_bridge = initial_at_bridge - len(artist_track_bridge)
    print(f"Artist-Track bridge: {removed_at_bridge} eliminados, {len(artist_track_bridge)} mantenidos")
    
    initial_gt_bridge = len(genre_track_bridge)
    genre_track_bridge = genre_track_bridge[genre_track_bridge['track_id'].isin(used_track_ids)]
    removed_gt_bridge = initial_gt_bridge - len(genre_track_bridge)
    print(f"Genre-Track bridge: {removed_gt_bridge} eliminados, {len(genre_track_bridge)} mantenidos")
    
    used_genre_ids = set(genre_track_bridge['genre_id'].unique())
    initial_genres = len(genre_dim)
    genre_dim = genre_dim[genre_dim['genre_id'].isin(used_genre_ids)]
    removed_genres = initial_genres - len(genre_dim)
    print(f"Géneros: {removed_genres} eliminados, {len(genre_dim)} mantenidos")
    
    print("\n=== REPORTE DE INTEGRIDAD REFERENCIAL ===")
    print(f"Grammy events: {len(grammy_event_dim)}")
    print(f"Artistas ganadores: {len(artist_dim)}")
    print(f"Tracks ganadores: {len(track_dim)}")
    print(f"Géneros: {len(genre_dim)}")
    print(f"Artist-Track bridges: {len(artist_track_bridge)}")
    print(f"Genre-Track bridges: {len(genre_track_bridge)}")
    print(f"\nPremios totales: {len(award_fact)}")
    print(f"  - Solo artista: {len(award_fact[award_fact['winner_artist_id'].notna() & award_fact['winner_track_id'].isna()])}")
    print(f"  - Solo track: {len(award_fact[award_fact['winner_track_id'].notna() & award_fact['winner_artist_id'].isna()])}")
    print(f"  - Ambos: {len(award_fact[award_fact['winner_artist_id'].notna() & award_fact['winner_track_id'].notna()])}")
    print("==========================================\n")
    
    dw = {
        "track_dim": track_dim,
        "artist_dim": artist_dim,
        "artist_track_bridge": artist_track_bridge,
        "genre_dim": genre_dim,
        "genre_track_bridge": genre_track_bridge,
        "grammy_event_dim": grammy_event_dim,
        "award_fact": award_fact
    }
    return dw


def classify_category(cat: str) -> str:
    if pd.isna(cat):
        return 'artist' 
    cat = str(cat).lower()
    if any(w in cat for w in ['song', 'record', 'track', 'single']):
        return 'track'
    if any(w in cat for w in ['artist', 'album', 'group', 'duo', 'band', 'performer', 'vocalist']):
        return 'artist'
    return 'artist'

KNOWN_TRACKS = {
    'speechless',
    'friends', 
    'unleashed',
    'gravity',
    'home',
    'heaven',
    'matrix',
    'the score',
    'the gathering',
    'plan b',
    'crying',
    'passion',
    'love',
    'eve',
    'tommy',
    'gece'
}


def build_award_candidates(award_fact_df: pd.DataFrame, artist_dim: pd.DataFrame, track_dim: pd.DataFrame):
    artist_candidates = award_fact_df[award_fact_df['category_type'] == 'artist'].copy()
    artist_candidates = artist_candidates[~artist_candidates['name'].str.lower().isin(KNOWN_TRACKS)]
    
    artist_matches = (
        artist_candidates
        .merge(artist_dim, left_on='name', right_on='name', how='inner')
        .drop_duplicates(subset=['award_fact_id', 'artist_id'])
    )
    artist_matches['match_type'] = 'artist'
    
    track_candidates = award_fact_df[
        (award_fact_df['category_type'] == 'track') | 
        (award_fact_df['name'].str.lower().isin(KNOWN_TRACKS))
    ].copy()
    
    track_matches = (
        track_candidates
        .merge(track_dim, left_on='name', right_on='track_name', how='inner')
        .drop_duplicates(subset=['award_fact_id', 'track_id'])
    )
    track_matches['match_type'] = 'track'
    
    print(f"Coincidencias encontradas:")
    print(f"  Artistas: {len(artist_matches)}")
    print(f"  Tracks: {len(track_matches)}")
    print(f"  Tracks conocidos bloqueados: {len(KNOWN_TRACKS)}")
    
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