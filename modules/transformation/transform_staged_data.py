import pandas as pd
import logging
import re

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from airflow.providers.postgres.hooks.postgres import PostgresHook
from log import log

@log
def transform_staged_data(postgres_conn_id, postgres_table):
    engine = create_engine(PostgresHook(postgres_conn_id=postgres_conn_id).get_uri())
    query = f"SELECT * FROM {postgres_table} WHERE department <> '' or status = ''"
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
        for _, row in df.iterrows():
            try:
                update_query = f"""
                    UPDATE {postgres_table}
                    SET department = %s,
                        status = %s
                    WHERE id = %s
                """
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