FROM prefecthq/prefect:2-latest
 # RUN apt-get install wget
RUN apt-get update && apt-get -y install wget libpq-dev gcc curl
RUN pip install pandas sqlalchemy psycopg2 pyarrow fastparquet "numpy<2" prefect prefect_sqlalchemy


WORKDIR /app
COPY ingest_data_parquet_prefect.py ingest_data_parquet_prefect.py


ENTRYPOINT [ "python", "ingest_data_parquet_prefect.py" ]
