import argparse, os, sys
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def extract_from_gcs(year: int, month: int) -> Path:
    """Download trip data from GCS"""
    wd = os.getcwd()
    print(f"Working directory: {wd}")
    gcs_path = f"data/yellow_tripdata_{year}-{month:02}_clean.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    print(f"./{gcs_path}")
    return Path(f"./{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame, year: int, month: int) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    destination_table=f"dezoomcamp.yellow_tripdata_{year}-{month:02}_clean"
    print(f"Destination table: {destination_table}")

    df.to_gbq(
        destination_table=destination_table,
        project_id="steady-webbing-436216-k1",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int) -> None:
    path = extract_from_gcs(year, month)
    df = transform(path)
    write_bq(df, year, month)
    print('Process completed successfully.')


@flow(log_prints=True)
def etl_gcs_to_bq_parent_flow():
    """The main ETL function"""
    # parser = argparse.ArgumentParser(description='Ingest Parquet data to GCS Bucket')
    # parser.add_argument('--start_month', required=True, help='start month of data')
    # parser.add_argument('--end_month', required=True, help='end month of data')
    # args = parser.parse_args()

    start_month = 6 #int(args.start_month)
    end_month = 7 #int(args.end_month)
    year = 2024

    for month in range(start_month, end_month+1):
        etl_gcs_to_bq(year, month)




if __name__ == "__main__":
    etl_gcs_to_bq_parent_flow()
