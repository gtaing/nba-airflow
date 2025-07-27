import os
import s3fs
import polars as pl
import pyarrow.fs as fs

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from loguru import logger
from polars import LazyFrame
from pyarrow.dataset import dataset


class S3Bucket(object):
    """
    A class to handle S3 bucket operations.
    """

    def __init__(self):
        self.aws_conn_id = "aws_default"
        self.region_name = os.getenv("AWS_REGION_NAME", "eu-west-3")
        self.bucket_name = os.getenv("S3_BUCKET_NAME")

        hook = S3Hook(self.aws_conn_id)
        creds = hook.get_credentials()

        self.storage_options = {"key": creds.access_key, "secret": creds.secret_key}
        self.fs = s3fs.S3FileSystem(**self.storage_options)

    def __repr__(self):
        return f"S3Bucket(bucket_name={self.bucket_name})"

    def scan_pyarrow_dataset(self, filepath: str) -> LazyFrame:
        """
        Scan for Parquet files in the specified S3 bucket and prefix using PyArrow.
        """

        logger.info(f"Scanning Parquet dataset: s3://{self.bucket_name}/{filepath}")

        s3_fs = fs.S3FileSystem(
            access_key=self.storage_options["key"],
            secret_key=self.storage_options["secret"],
            region=self.region_name,
        )

        ds = dataset(
            f"{self.bucket_name}/{filepath}", filesystem=s3_fs, format="parquet"
        )

        return pl.scan_pyarrow_dataset(ds)


    def sink_parquet_to_s3(
        self, lf: LazyFrame, output_key: str, folder: str = "processed"
    ) -> None:
        """
        Write a Polars LazyFrame to S3 in Parquet format.
        """

        logger.info(f"Writing data to s3://{self.bucket_name}/{folder}/{output_key}")

        with self.fs.open(f"s3://{self.bucket_name}/{folder}/{output_key}", "wb") as f:
            lf.collect().write_parquet(
                f, compression="snappy", storage_options=self.storage_options
            )

        logger.info(f"Data written to s3://{self.bucket_name}/{output_key}")


nba_bucket = S3Bucket()
