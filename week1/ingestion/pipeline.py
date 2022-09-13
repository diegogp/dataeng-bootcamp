from tabnanny import verbose
import pandas as pd
from sqlalchemy import create_engine
import argparse

def main(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table_name = args.table_name
    #url = args.url
    #csv_name = 'output.csv'

    #os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_parquet("/code/raw_data/yellow_tripdata_2021-01.parquet")

    df.to_sql(name=table_name,\
            con=engine, index=False,\
            if_exists='replace', chunksize=100000)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest PARQUET data to Postgresql.')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    #parser.add_argument('--url', required=True, help='url of the parquet file')


    args = parser.parse_args()
    main(args)