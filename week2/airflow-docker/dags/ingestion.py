import pandas as pd
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, parquet_file):

    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')
    print(engine.connect())
    print(parquet_file)
    df = pd.read_parquet(parquet_file)
    try:
        df.to_sql(name=table_name,\
                con=engine, index=False,\
                if_exists='replace', chunksize=100000)
        print('connection established successfully, inserting data...')
    except ConnectionError:
        print("Error while ingesting data.")