import argparse, os, sys
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def extract_from_gcs(year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/yellow_tripdata_{year}-{month:02}_clean.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    # data_to_bq directory needs to exist in the current working directory
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data_to_bq")
    return Path(f"./data_to_bq/{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="steady-webbing-436216-k1",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq():
    """The main ETL function"""
    parser = argparse.ArgumentParser(description='Ingest Parquet data to GCS Bucket')

    parser.add_argument('--start_month', required=True, help='start month of data')
    args = parser.parse_args()
    start_month = int(args.start_month)
    year = 2024

    path = extract_from_gcs(year, start_month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()