import polars as pl

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
