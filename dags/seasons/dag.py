from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from games.games_scope import save_game_id_scope_in_tmp
from players.season_stats import compute_season_stats
from teams.season_stats import get_team_season_stats

with DAG(
    dag_id="season_stats",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["nba"]
) as dag:
    
    get_scope_of_games = PythonOperator(
        task_id="get_scope_of_games",
        python_callable=save_game_id_scope_in_tmp
    )
    
    players_stats = PythonOperator(
        task_id="compute_players_stats",
        python_callable=compute_season_stats
    )

    teams_stats = PythonOperator(
        task_id="compute_team_stats",
        python_callable=get_team_season_stats
    )

    get_scope_of_games >> [players_stats, teams_stats]

