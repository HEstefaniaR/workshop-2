import pandas as pd


def merge_dw(spotify_dims: dict[str, pd.DataFrame], grammy_dims: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    track_dim = spotify_dims["track_dim"]
    artist_dim = spotify_dims["artist_dim"]
    artist_track_bridge = spotify_dims["artist_track_bridge"]
    genre_dim = spotify_dims["genre_dim"]
    genre_track_bridge = spotify_dims["genre_track_bridge"]
    grammy_event_dim = grammy_dims["grammy_event_dim"]
    award_fact_df = grammy_dims["award_fact"]
    
    award_fact_df['track_name'] = award_fact_df.get('track_name', pd.Series()).replace(['nan', ''], pd.NA)
    award_fact_df['name'] = award_fact_df['name'].replace(['nan', ''], pd.NA)
    
    artist_matches, track_matches = build_award_candidates(award_fact_df, artist_dim, track_dim)
    award_fact = build_award_fact(artist_matches, track_matches, grammy_event_dim)
    
    print("\n=== REPORTE DE INTEGRIDAD REFERENCIAL ===")
    print(f"Grammy events únicos: {len(grammy_event_dim)}")
    print(f"Grammy events con premios: {award_fact['grammy_event_id'].nunique()}")
    print(f"Grammy events sin premios: {len(grammy_event_dim) - award_fact['grammy_event_id'].nunique()}")
    print(f"\nPremios totales: {len(award_fact)}")
    print(f"Premios solo con artista: {len(award_fact[award_fact['winner_artist_id'].notna() & award_fact['winner_track_id'].isna()])}")
    print(f"Premios solo con track: {len(award_fact[award_fact['winner_track_id'].notna() & award_fact['winner_artist_id'].isna()])}")
    print(f"Premios con ambos: {len(award_fact[award_fact['winner_artist_id'].notna() & award_fact['winner_track_id'].notna()])}")
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


def build_award_candidates(award_fact_df: pd.DataFrame, artist_dim: pd.DataFrame, track_dim: pd.DataFrame):
    artist_matches = (
        award_fact_df.merge(
            artist_dim,
            left_on='name',
            right_on='name',
            how='left'
        )
        .drop_duplicates(subset=['award_fact_id', 'artist_id'])
    )
    artist_matches['match_type'] = 'artist'
    
    track_matches = (
        award_fact_df.merge(
            track_dim,
            left_on='name',
            right_on='track_name',
            how='left'
        )
        .drop_duplicates(subset=['award_fact_id', 'track_id'])
    )
    track_matches['match_type'] = 'track'
    
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