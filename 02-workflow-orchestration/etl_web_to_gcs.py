from pathlib import Path
import pandas as pd
import argparse, os, sys
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(log_prints=True)
def download_data(url: str, file_name: str) -> pd.DataFrame:

   # Get the name of the file from url
    # file_name = url.rsplit('/', 1)[-1].strip()
    print(f'Downloading {file_name} ...')
    # Download file from url
    os.system(f'curl {url.strip()} -o {file_name}')
    print('\n')

    #get the current working directory
    cwd = os.getcwd()


    file_path = "%s/%s" % (cwd, file_name)
    df = pd.read_parquet(file_path, engine='fastparquet')

    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = f"data/{dataset_file.stem}_clean.parquet"
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    parser = argparse.ArgumentParser(description='Ingest Parquet data to GCS Bucket')

    parser.add_argument('--start_month', required=True, help='start month of data')
    # parser.add_argument('--end_month', required=True, help='end month of data')
    args = parser.parse_args()
    start_month = args.start_month
    
    year = 2024

    if len(str(args.start_month)) == 1:
        start_month = "".join(["0", str(start_month)])
    else:
        start_month = str(start_month)
    dataset_file = Path(f"yellow_tripdata_{year}-{start_month}.parquet")
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"

    df = download_data(dataset_url, dataset_file)
    # df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()