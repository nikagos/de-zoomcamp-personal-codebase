from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/nikagos/de-zoomcamp-personal-codebase.git",
        entrypoint="02-workflow-orchestration/test_flow_deployment.py:buy"
    ).deploy(
        name="my-github-deployment",
        work_pool_name="my-work-pool",        
    )