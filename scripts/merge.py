import pandas as pd


def merge_dw(spotify_dims: dict[str, pd.DataFrame], grammy_dims: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    track_dim = spotify_dims["track_dim"]
    artist_dim = spotify_dims["artist_dim"]
    track_artist_dim = spotify_dims["track_artist_dim"]
    genre_dim = spotify_dims["genre_dim"]
    genre_song_dim = spotify_dims["genre_song_dim"]

    grammy_event_dim = grammy_dims["grammy_event_dim"]
    nominee_dim = grammy_dims["provisional_nominee_dim"]

    artist_matches, track_matches = build_award_candidates(nominee_dim, artist_dim, track_dim)
    award_fact = build_award_fact(artist_matches, track_matches)

    dw = {
        "track_dim": track_dim,
        "artist_dim": artist_dim,
        "track_artist_dim": track_artist_dim,
        "genre_dim": genre_dim,
        "genre_song_dim": genre_song_dim,
        "grammy_event_dim": grammy_event_dim,
        "provisional_nominee_dim": nominee_dim,
        "award_fact": award_fact
    }
    return dw


def build_award_candidates(nominee_dim: pd.DataFrame, artist_dim: pd.DataFrame, track_dim: pd.DataFrame):

    artist_matches = (
        nominee_dim.merge(
            artist_dim,
            left_on='name',
            right_on='name',
            how='inner'
        )
        .drop_duplicates(subset=['nominee_id', 'artist_id'])
    )
    artist_matches['match_type'] = 'artist'

    track_matches = (
        nominee_dim.merge(
            track_dim,
            left_on='name',
            right_on='track_name',
            how='inner'
        )
        .drop_duplicates(subset=['nominee_id', 'track_id'])
    )
    track_matches['match_type'] = 'track'

    print("Coincidencias encontradas:")
    print(f"Artistas: {len(artist_matches)}")
    print(f"Tracks: {len(track_matches)}")

    return artist_matches, track_matches


def build_award_fact(artist_matches: pd.DataFrame, track_matches: pd.DataFrame) -> pd.DataFrame:

    artist_awards = artist_matches[['artist_id', 'grammy_event_id', 'winner']].copy()
    artist_awards['track_id'] = None
    artist_awards = artist_awards[['artist_id', 'track_id', 'grammy_event_id', 'winner']]

    track_awards = track_matches[['track_id', 'grammy_event_id', 'winner']].copy()
    track_awards['artist_id'] = None
    track_awards = track_awards[['artist_id', 'track_id', 'grammy_event_id', 'winner']]

    award_fact = pd.concat([artist_awards, track_awards], ignore_index=True)
    award_fact.insert(0, 'award_id', range(1, len(award_fact) + 1))

    return award_fact

