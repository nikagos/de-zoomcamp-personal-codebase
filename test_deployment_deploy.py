from prefect import flow


@flow
def my_flow():
    print("Hello, Prefect!")

if __name__ == "__main__":
    my_flow.deploy(
        name="my-second-deployment",
        work_pool_name="test-work-processor", # created in advance
        image="my-image",
        push=False
        #, cron="* * * * *""
    )
