from prefect import flow
from prefect.deployments.runner import DockerImage
from parameterized_flow import etl_parent_flow


@flow(log_prints=True)
def my_flow():
    etl_parent_flow()


if __name__ == "__main__":
    my_flow.from_source(
        source="https://github.com/nikagos/de-zoomcamp-personal-codebase.git",
        entrypoint="02-workflow-orchestration/parameterized_flow.py:etl_parent_flow"
    ).deploy(
        name="parameterized-flow-deployment",
        work_pool_name="my-work-pool",
        image=DockerImage(
            name="nikagos/my-parameterized-flow-image",
            tag="v001",
            dockerfile="/home/nickk/de-zoomcamp-personal-codebase/02-workflow-orchestration/Dockerfile-parameterized-flow"
        ),
        push=True
    )