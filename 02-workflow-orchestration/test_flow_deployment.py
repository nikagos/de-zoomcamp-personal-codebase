from prefect import flow

@flow(log_prints=True)
def buy():
    print("Bought shares!")

if __name__ == "__main__":
    buy()