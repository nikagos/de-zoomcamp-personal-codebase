from pathlib import Path
import pandas as pd
import argparse, os, sys
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
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
    # print(df.head(10))

    return df


@task(log_prints=True)
def clean(vehicle_type: str, df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    if vehicle_type == 'yellow':
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])        
    print(df.head(2))
    # print(f"columns: {df.dtypes}")
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
def etl_web_to_gcs(vehicle_type: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = Path(f"{vehicle_type}_tripdata_{year}-{month:02}.parquet")
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"

    df = download_data(dataset_url, dataset_file)
    df_clean = clean(vehicle_type, df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow() -> None:

    vehicle_type = ['yellow', 'green']

    # testing git creds
    #parser = argparse.ArgumentParser(description='Ingest Parquet data to GCS Bucket')
    
    #parser.add_argument('--start_month', required=True, help='start month of data')
    #parser.add_argument('--end_month', required=True, help='end month of data')
    #args = parser.parse_args()
    start_month = 6 #int(args.start_month)
    end_month = 7 #int(args.end_month)
    
    year = 2024

    for vehicle_type in vehicle_type:
        for month in range(start_month, end_month+1):
            etl_web_to_gcs(vehicle_type, year, month)


if __name__ == "__main__":
    etl_parent_flow()