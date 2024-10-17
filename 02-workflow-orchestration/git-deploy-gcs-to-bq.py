from prefect import flow
from prefect.deployments.runner import DockerImage
from etl_gcs_to_bq_dockerized import etl_gcs_to_bq_parent_flow


@flow(log_prints=True)
def my_flow():
    etl_gcs_to_bq_parent_flow()


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