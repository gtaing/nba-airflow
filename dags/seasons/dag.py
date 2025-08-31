from airflow.decorators import dag, task
from datetime import datetime


from config.motherduck import DuckDB
from players.season_stats import get_player_season_stats
from teams.season_stats import get_team_season_stats

@dag(
        start_date=datetime(2023,1,1), 
        description="DAG to compute and export NBA stats from S3 to DuckDB/MotherDuck.",
        catchup=False, 
        tags=["nba"]
    )
def season_stats():

    player_stats_fpath = get_player_season_stats()
    team_stats_fpath = get_team_season_stats()

    duckdb = DuckDB()

    @task
    def export_player_stats_to_duckdb(player_stats_fpath):
        duckdb.create_table_from_file(player_stats_fpath, 'players_stats')

    @task
    def export_team_stats_to_duckdb(team_stats_fpath):
        duckdb.create_table_from_file(team_stats_fpath, 'teams_stats')

    export_player_stats_to_duckdb(player_stats_fpath)
    export_team_stats_to_duckdb(team_stats_fpath)

season_stats()
