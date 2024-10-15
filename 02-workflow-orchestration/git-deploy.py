from prefect import flow


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/nikagos/de-zoomcamp-personal-codebase.git",
        # entrypoint="02-workflow-orchestration/parameterized_flow.py:etl_parent_flow"
        entrypoint="02-workflow-orchestration/test_pandas.py:pandas_flow"
    ).deploy(
        name="test-pandas-deployment",
        work_pool_name="my-work-pool",
    )