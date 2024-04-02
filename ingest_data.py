import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import urlparse

def ingest(params):
    # Param
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    #Download Data
    os.system(f"wget {url}")
    csv_name = os.path.basename(urlparse(url).path)

    #Connect to Database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    #Bulid the New Table
    df = pd.read_csv(csv_name)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    #Insert Data
    df = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    for chunk_df in df:
        t_start = time()

        chunk_df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print('Inserted chunk, took %.3f second' % (t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    ingest(args)

