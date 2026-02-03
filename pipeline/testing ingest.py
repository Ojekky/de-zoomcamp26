#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from tqdm.auto import tqdm

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table):
    # Updated URL to the Cloudfront Parquet source
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.parquet'
    
    # Optional: If you strictly need a CSV copy locally
    csv_name = f'yellow_tripdata_{year}-{month:02d}.csv'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(f'Downloading and processing {url}...')
    
    # Use PyArrow to read the Parquet file
    parquet_file = pq.ParquetFile(url)
    
    # We can still iterate in batches to save memory
    first = True
    for batch in tqdm(parquet_file.iter_batches(batch_size=100000)):
        df_chunk = batch.to_pandas()
        
        # Save to CSV (first time creates/overwrites, then appends)
        if first:
            df_chunk.to_csv(csv_name, index=False)
            # Create the table in Postgres
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
            first = False
        else:
            df_chunk.to_csv(csv_name, mode='a', header=False, index=False)

        # Ingest into Postgres
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

    print(f'Successfully ingested data and created {csv_name}')

if __name__ == '__main__':
    run()