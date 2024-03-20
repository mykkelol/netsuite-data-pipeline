import pandas as pd
import logging
import re
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from airflow.providers.postgres.hooks.postgres import PostgresHook
from log import log

def get_sql_path(subfix):
    return f'sql/{Path(__file__).stem}_{subfix}.sql'

@log
def transform_staged_data(postgres_conn_id, postgres_table):
    with open(get_sql_path('query'), 'r') as f:
        query_file = f.read()
        
    engine = create_engine(PostgresHook(postgres_conn_id=postgres_conn_id).get_uri())
    query = query_file.format(postgres_table=postgres_table)
    df = pd.read_sql(query, engine)
    
    if df.empty:
        logging.info(f'No rows to transform. Finishing task.')
        return False
    
    df['department'] = df['department'].apply(
        lambda x: re.sub(r'^\d+\s', '', x) if isinstance(x, str) else x
    )
    df['status'] = df['status'].apply(
        lambda x: 'Approved' if not x or x.strip() == '' else x
    )

    errors = []
    with engine.begin() as conn:
        with open(get_sql_path('update'), 'r') as f:
            query_file = f.read()
            
        for _, row in df.iterrows():
            try:
                update_query = query_file.format(postgres_table=postgres_table)
                conn.execute(
                    update_query,
                    (
                        row['department'],
                        row['status'],
                        row['id']
                    )
                )
            except SQLAlchemyError as e:
                errors.append((row['id'], str(e)))

    if errors:
        raise ValueError(f"Error while updating rows: \n{errors}")
    
    logging.info(f'SUCCESS: {postgres_table} table transformed with {postgres_conn_id}')
    return True