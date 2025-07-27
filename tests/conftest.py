import os
import pytest

from tests import TARGET_DIR
from config import bucket


@pytest.fixture(autouse=True)
def mock_nba_bucket(monkeypatch, request):
        # Patch the sink_parquet_to_s3 method to write to /target/<test_name>/
    def fake_sink_parquet_to_s3(df, output_key):
        test_dir_inside_target = os.path.join(TARGET_DIR, request.node.name)
        os.makedirs(test_dir_inside_target, exist_ok=True)
        output_path = os.path.join(test_dir_inside_target, output_key)
        
        df.collect().write_parquet(output_path)

    monkeypatch.setattr(bucket.nba_bucket, "sink_parquet_to_s3", fake_sink_parquet_to_s3)