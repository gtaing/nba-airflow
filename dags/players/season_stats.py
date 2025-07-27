import polars as pl

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
from config.bucket import nba_bucket
from games.games_scope import compute_games_scope_task
from players import PLAYERS_METRICS


def scan_players_game_stats() -> None:
    """
    Scan the player game statistics CSV file from S3.
    """
    lf = nba_bucket.scan_pyarrow_dataset(filepath="raw/playerstatistics.parquet")

    # Save to temporary Parquet file to pass to next task
    tmp_path = "/tmp/player_stats.parquet"

    (
        lf
        .filter(pl.col("gameDate").str.to_datetime().dt.year() >= 2014)
        .collect()
        .write_parquet(tmp_path)
    )


def compute_season_stats() -> None:
    """
    Get the players' season statistics by aggregating game stats.
    """

    players_stats = pl.scan_parquet("/tmp/player_stats.parquet")
    game_id_scope = pl.scan_parquet("/tmp/game_id_scope.parquet")

    dimensions = ["season", "firstName", "lastName", "personId", "gameType"]
    number_of_games_played = pl.col("gameId").n_unique().alias("GP")

    average_metrics = [
        pl.mean(metric).round(1).alias(alias)
        for metric, alias in PLAYERS_METRICS.items()
    ]

    season_stats = (
        players_stats.join(
            game_id_scope, how="inner", left_on="gameId", right_on="game_id"
        )
        .group_by(*dimensions)
        .agg(number_of_games_played, *average_metrics)
    )

    nba_bucket.sink_parquet_to_s3(season_stats, "player_season_stats.parquet")


with DAG(
    dag_id="player_season_stats",
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['nba']) as dag:
    
    scan_player_stats = PythonOperator(
        task_id="load_players_stats",
        python_callable=scan_players_game_stats
    )

    compute_player_season_stats = PythonOperator(
        task_id="compute_season_stats",
        python_callable=compute_season_stats
    )

    [compute_games_scope_task, scan_player_stats] >> compute_player_season_stats
