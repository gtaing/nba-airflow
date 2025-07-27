import os
import pytest
import datetime as datetime
import polars as pl

from polars.testing import assert_frame_equal

from dags.teams.season_stats import get_transformed_games, create_full_games, compute_team_season_stats, get_team_season_stats
from dags.teams import season_stats

from tests import TARGET_DIR


@pytest.fixture
def mock_team_config_map(monkeypatch):
    monkeypatch.setattr(
        season_stats,
        "TEAM_CONFIG_MAP",
        {
            "home": {
                "renaming": {
                    "team_abbreviation_home": "team", 
                    "team_name_home": "team_name"
                    },
                "team_metrics": {
                    "pts_home": "team_pts"
                    },
                "opponent_metrics": {
                    "pts_away": "opponent_pts"
                    },
            }
        }
    )


@pytest.fixture
def mock_team_metrics(monkeypatch):
    monkeypatch.setattr(
        season_stats,
        "TEAM_METRICS",
        value=["pts", "reb"]
    )


def test_get_transformed_games_home(mock_team_config_map) -> None:
    games_details = pl.LazyFrame({
        "game_id": [1, 2],
        "season_id": [2023, 2023],
        "season_type": ["Regular Season", "Pre Season"],
        "game_date": ["2023-01-01", "2023-01-02"],
        "team_abbreviation_home": ["TeamA", "TeamB"],
        "team_name_home": ["Team A", "Team B"],
        "pts_home": [100, 110],
        "pts_away": [90, 105],
    })

    result = get_transformed_games(games_details, "home").collect()

    expected = pl.DataFrame({
        "game_id": [1],
        "season_id": [2023],
        "season_type": ["Regular Season"],
        "game_date": [datetime.datetime(2023, 1, 1)],
        "game_location": ["home"],
        "team": ["TeamA"],
        "team_name": ["Team A"],
        "team_pts": [100],
        "opponent_pts": [90],
    })

    assert_frame_equal(result, expected, check_dtypes=False)


def test_create_full_games() -> None:
    home_games = pl.LazyFrame({"game_id": [1], "location": "home"})
    away_games = pl.LazyFrame({"game_id": [2], "location": "away"})
    game_summary = pl.LazyFrame({"game_id": [1, 2], "season": [2023, 2001]})

    expected = pl.DataFrame({"game_id": [1], "location": ["home"], "season": [2023]})

    result = create_full_games(home_games=home_games,
                               away_games=away_games,
                               game_summary=game_summary)
    
    assert_frame_equal(result.collect(), expected)


def test_compute_team_season_stats(mock_team_metrics) -> None:
    full_games = pl.LazyFrame({
        "season_id": [2023, 2023, 2023],
        "team": ["A", "A", "B"],
        "team_name": ["Alpha", "Alpha", "Beta"],
        "season": [2023, 2023, 2023],
        "game_id": [1, 2, 3],
        "win_loss": ["W", "L", "W"],
        "team_pts": [100, 90, 110],
        "team_reb": [50, 45, 55],
        "opponent_pts": [95, 100, 100],
        "opponent_reb": [48, 47, 50],
    })

    expected = pl.DataFrame({
        "season_id": [2023, 2023],
        "team": ["A", "B"],
        "team_name": ["Alpha", "Beta"],
        "season": [2023, 2023],
        "wins": [1, 1],
        "losses": [1, 0],
        "total_games": [2, 1],
        "team_pts": [95.0, 110.0],
        "team_reb": [47.5, 55.0],
        "opponent_pts": [97.5, 100.0],
        "opponent_reb": [47.5, 50.0],
    })


    result = compute_team_season_stats(full_games).collect()


    assert_frame_equal(result, 
                       expected, 
                       check_dtypes=False, 
                       check_column_order=False, 
                       check_row_order=False)
    


def test_get_team_season_stats(mock_nba_bucket, monkeypatch, request):
    # Mock scan_pyarrow_dataset and pl.scan_parquet to return small test data
    monkeypatch.setattr(
        season_stats.nba_bucket,
        "scan_pyarrow_dataset",
        lambda _: pl.LazyFrame({
            "game_id": [1, 2],
            "season_id": [2023, 2023],
            "season_type": ["Regular Season", "Regular Season"],
            "game_date": ["2023-01-01", "2023-01-02"],
            "wl_away": ["W", "L"],
            "team_abbreviation_home": ["A", "B"],
            "team_name_home": ["Alpha", "Beta"],
            "pts_home": [100, 110],
            "pts_away": [90, 105],
        })
    )
    monkeypatch.setattr(
        pl,
        "scan_parquet",
        lambda _: pl.LazyFrame({
            "game_id": [1, 2],
            "season": [2023, 2023]
        })
    )
    # Patch TEAM_CONFIG_MAP and TEAM_METRICS for the test
    monkeypatch.setattr(
        season_stats,
        "TEAM_CONFIG_MAP",
        {
            "home": {
                "renaming": {
                    "team_abbreviation_home": "team", 
                    "team_name_home": "team_name",
                    "wl_away": "win_loss"
                    },
                "team_metrics": {"pts_home": "team_pts"},
                "opponent_metrics": {"pts_away": "opponent_pts"},
            },
            "away": {
                "renaming": {
                    "team_abbreviation_home": "team", 
                    "team_name_home": "team_name",
                    "wl_away": "win_loss"
                    },
                "team_metrics": {"pts_away": "team_pts"},
                "opponent_metrics": {"pts_home": "opponent_pts"},
            }
        }
    )
    monkeypatch.setattr(season_stats, "TEAM_METRICS", ["pts"])

    # Run the function
    get_team_season_stats()

    # Check that the parquet file was written in the test-specific folder
    test_name = request.node.name
    output_path = os.path.join(TARGET_DIR, test_name, "team_season_stats.parquet")

    assert os.path.exists(output_path)

    df = pl.read_parquet(str(output_path))
    assert "team" in df.columns
    assert "team_pts" in df.columns