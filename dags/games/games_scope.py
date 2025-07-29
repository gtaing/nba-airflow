import polars as pl

from airflow.providers.standard.operators.python import PythonOperator
from config.bucket import nba_bucket


def get_game_id_in_scope() -> pl.LazyFrame:
    """
    Get the game IDs for seasons starting from 2015.
    """
    game_summary = nba_bucket.scan_parquet("raw/game_summary.parquet")

    return (
        game_summary
        .filter(pl.col("season") >= 2015)
        .select(
            "season", 
            "game_id"
        )
    )



def save_game_id_scope_in_tmp() -> None:
    """
    Save the game ID scope to a temporary Parquet file.
    """
    game_id_scope = get_game_id_in_scope()
    
    # Save to temporary Parquet file
    tmp_path = "/tmp/game_id_scope.parquet"
    game_id_scope.collect().write_parquet(tmp_path)

    return tmp_path


compute_games_scope_task = PythonOperator(
    task_id="compute_games_scope",
    python_callable=save_game_id_scope_in_tmp)