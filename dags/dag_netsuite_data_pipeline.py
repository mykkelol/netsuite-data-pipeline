from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.helpers import chain

from dags_config import Config as config
from custom_operators import (
    NetSuiteToS3Operator,
    S3ToPostgresTransferOperator
)

def dummy_callable(action):
    return (
        f'{datetime.now()}: {action} '
        f'NetSuite GL posting transactions data pipeline'
    )

def extract_netsuite_results(
        config,
        search_type,
        search_id,
        dag,
    ):
    return NetSuiteToS3Operator(
        task_id=f'extract_{search_id}',
        search_types=config.SUPPORTED_RECORD_TYPES,
        search_type=search_type,
        search_id=search_id,
        conn_id=config.S3_CONN_ID,
        bucket_name=config.LANDING_BUCKET,
        filename='netsuite_extracts_{{ ds_nodash }}',
        filter_expression=[
            ['taxline','is','F'], 
            'AND', 
            ['cogs','is','F'], 
            'AND', 
            ['posting','is','T'], 
            'AND', 
            ['accountingbook','anyof',config.ACCOUNTING_BOOKS['secondary']]
        ],
        columns=None,
        dag=dag
    )

def create_dag(dag_id, interval, config, search_type, searches):
    with DAG(
        dag_id=dag_id,
        description=f'Ingested new GL posting {{search_type}} from NetSuite',
        schedule_interval=interval,
        start_date=datetime(2024, 2, 25, 2),
        catchup=False
    ) as dag:
        
        start = PythonOperator(
            task_id='starting_pipeline',
            python_callable=dummy_callable,
            op_kwargs={'action': 'starting'},
            dag=dag
        )

        load_netsuite_to_s3_landing = [
            extract_netsuite_results(
                config=config,
                search_type=search_type,
                search_id=search_id,
                dag=dag
            )
            for search_id in searches
        ]

        truncate_postgres_stage = [ 
            PostgresOperator(
                task_id=f'truncate_postgres_{search_id}',
                postgres_conn_id=config.POSTGRES_CONN_ID,
                sql=f'TRUNCATE TABLE {search_id}',
            )
            for search_id in searches
        ]

        load_s3_landing_to_postgres_stage = [ 
            S3ToPostgresTransferOperator(
                task_id=f'load_to_postgres_stage_{search_id}',
                aws_conn_id=config.S3_CONN_ID,
                s3_key='netsuite_extracts_{{ ds_nodash }}.csv',
                s3_bucket_name=config.LANDING_BUCKET,
                postgres_conn_id=config.POSTGRES_CONN_ID,
                postgres_table=search_id,
                dag=dag
            )
            for search_id in searches
        ]
        
        finish = PythonOperator(
            task_id='finishing_pipeline',
            python_callable=dummy_callable,
            op_kwargs={'action': 'finishing'},
        )

        chain(
            start,
            load_netsuite_to_s3_landing,
            truncate_postgres_stage,
            load_s3_landing_to_postgres_stage,
            finish
        )
    
    return dag

if len(config.TRANSACTION_TYPES) > 5:
    raise ValueError('Exceeds NetSuite SuiteScript usage governance limit')

for i, item in enumerate(config.TRANSACTION_TYPES.items()):
    search_type, searches = item
    dag_id = f'netsuite_data_pipeline_{search_type}'
    interval = f'{str(i * 10)} * * * *'

    globals()[dag_id] = create_dag(
        dag_id,
        interval,
        config,
        search_type,
        searches
    )