import argparse
from prefect import flow
from prefect.deployments.runner import DockerImage
from etl_gcs_to_bq_dockerized import etl_gcs_to_bq_parent_flow


@flow(log_prints=True)
def my_flow():

    parser = argparse.ArgumentParser(description='Ingest Parquet data to GCS Bucket')
    parser.add_argument('--start_month', required=True, help='start month of data')
    parser.add_argument('--end_month', required=True, help='end month of data')
    args = parser.parse_args()
    start_month = int(args.start_month)
    end_month = int(args.end_month)
    year = 2024

    etl_gcs_to_bq_parent_flow(year, start_month, end_month)


if __name__ == "__main__":
    my_flow.from_source(
        source="https://github.com/nikagos/de-zoomcamp-personal-codebase.git",
        entrypoint="02-workflow-orchestration/etl_gcs_to_bq_dockerized.py:etl_gcs_to_bq_parent_flow"
    ).deploy(
        name="etl-gcs-to-bq-flow-deployment",
        work_pool_name="my-work-pool",
        image=DockerImage(
            name="nikagos/my-etl-gcs-to-bq-flow-image",
            tag="v001",
            dockerfile="/home/nickk/de-zoomcamp-personal-codebase/02-workflow-orchestration/Dockerfile-web-to-bq"
        ),
        push=True
    )