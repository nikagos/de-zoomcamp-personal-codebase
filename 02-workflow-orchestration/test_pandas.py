import pandas as pd # pandas is not installed
from prefect import flow, task

@task
def create_and_print_dataframe():
    df = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6]
    })
    print(df)

@flow() # this causes an error during deployment
# @flow(name='test') # this works
def pandas_flow():
    create_and_print_dataframe()