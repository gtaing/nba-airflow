import os
import duckdb

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime

from players.season_stats import get_player_season_stats
from teams.season_stats import get_team_season_stats

@dag(start_date=datetime(2023,1,1), catchup=False, tags=["nba"])
def season_stats():

    player_stats = get_player_season_stats()
    team_stats   = get_team_season_stats()

    @task
    def create_duckdb_table(players_file: str, teams_file: str):
        token = os.getenv("motherduck_token")
        conn = duckdb.connect(f"md:my_db?motherduck_token={token}")

        aws_conn = BaseHook.get_connection("aws_default")
        aws_key = aws_conn.login
        aws_secret = aws_conn.password
        aws_region = os.getenv("AWS_REGION_NAME", "eu-west-3")

        # Configure DuckDB S3
        conn.execute(f"SET s3_region='{aws_region}'")
        conn.execute(f"SET s3_access_key_id='{aws_key}'")
        conn.execute(f"SET s3_secret_access_key='{aws_secret}'")


        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS players_stats
            AS SELECT * FROM '{players_file}';
        """)

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS teams_stats
            AS SELECT * FROM '{teams_file}';
        """)

    create_duckdb_table(player_stats, team_stats)

season_stats()

