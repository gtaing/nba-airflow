import polars as pl

from airflow.decorators import task
from config.bucket import nba_bucket
from games.games_scope import get_game_id_in_scope
from players import PLAYERS_METRICS

from polars import LazyFrame


def scan_players_game_stats() -> LazyFrame:
    """
    Scan the player game statistics CSV file from S3.
    """

    is_after_2014 = pl.col("gameDate").str.to_datetime().dt.year() >= 2014
 
    return (
        nba_bucket
        .scan_parquet(filepath="raw/playerstatistics.parquet")
        .filter(is_after_2014)
    )


def compute_player_season_stats(players_stats: LazyFrame, 
                                game_id_scope: LazyFrame) -> LazyFrame:
    """
    Compute season stats of NBA players.
    """

    dimensions = ["season", "firstName", "lastName", "personId", "gameType"]
    number_of_games_played = pl.col("gameId").n_unique().alias("GP")

    average_metrics = [
        pl.mean(metric).round(1).alias(alias)
        for metric, alias in PLAYERS_METRICS.items()
    ]

    return (
        players_stats
        .join(
            game_id_scope, 
            how="inner", 
            left_on="gameId", 
            right_on="game_id"
        )
        .group_by(*dimensions)
        .agg(
            number_of_games_played, 
            *average_metrics
        )
    )


@task
def get_player_season_stats() -> str:
    """
    Get the players' season statistics by aggregating game stats.
    """

    players_stats = scan_players_game_stats()
    game_id_scope = get_game_id_in_scope()

    season_stats = compute_player_season_stats(players_stats, 
                                               game_id_scope)

    output_path = nba_bucket.sink_parquet(season_stats, "player_season_stats.parquet")

    return output_path

