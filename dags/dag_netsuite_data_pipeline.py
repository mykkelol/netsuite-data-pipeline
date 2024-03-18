from datetime import datetime
from airflow import DAG
from airflow.utils.helpers import chain
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator

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

def create_bucket_key(base = 'netsuite_extracts', extension = '.csv'):
    return f'{base}_{{{{ ds_nodash }}}}{extension}'

def create_dag(
    dag_id,
    interval,
    config,
    search_type,
    search_id,
    filter
):
    with DAG(
        dag_id=dag_id,
        description=f'Ingested new GL posting {search_type} from NetSuite',
        default_view="graph",
        schedule_interval=interval,
        start_date=datetime(2024, 2, 25, 2),
        catchup=False,
        template_searchpath='sql'
    ) as dag:
        
        start = PythonOperator(
            task_id='starting_pipeline',
            python_callable=dummy_callable,
            op_kwargs={'action': 'starting'},
            dag=dag
        )

        load_netsuite_to_s3_landing = NetSuiteToS3Operator(
            task_id=f'extract_{search_id}',
            search_types=config.SUPPORTED_RECORD_TYPES,
            search_type=search_type,
            search_id=search_id,
            conn_id=config.S3_CONN_ID,
            bucket_name=config.LANDING_BUCKET,
            filename=create_bucket_key(extension=''),
            filter_expression=filter,
            columns=None,
            dag=dag
        )

        truncate_postgres_staging = PostgresOperator(
            task_id=f'truncate_postgres_{search_id}',
            postgres_conn_id=config.POSTGRES_CONN_ID,
            sql='truncate_postgres_staging.sql',
            params={'table_id': f'stage_{search_id}'}
        )

        load_s3_landing_to_postgres_staging = S3ToPostgresTransferOperator(
            task_id=f'load_to_postgres_stage_{search_id}',
            aws_conn_id=config.S3_CONN_ID,
            s3_key=create_bucket_key(extension='.csv'),
            s3_bucket_name=config.LANDING_BUCKET,
            postgres_conn_id=config.POSTGRES_CONN_ID,
            postgres_table=f'stage_{search_id}',
            dag=dag
        )

        load_postgres_staging_to_final = PostgresOperator(
            task_id=f'load_postgres_final_{search_id}',
            postgres_conn_id=config.POSTGRES_CONN_ID,
            sql='load_postgres_staging_to_final.sql',
            params={'search_id': search_id}
        )

        load_s3_landing_to_lake = S3CopyObjectOperator(
            task_id=f'load_s3_lake_{search_id}',
            aws_conn_id=config.S3_CONN_ID,
            source_bucket_key=f'S3://{config.LANDING_BUCKET}/{create_bucket_key()}',
            dest_bucket_key=f'S3://{config.LAKE_BUCKET}/{create_bucket_key()}',
            dag=dag,
        )

        delete_s3_landing = S3DeleteObjectsOperator(
            task_id=f'delete_s3_landing_{search_id}',
            aws_conn_id=config.S3_CONN_ID,
            bucket=config.LANDING_BUCKET,
            keys=create_bucket_key(extension='.csv'),
            dag=dag,
        )
        
        finish = PythonOperator(
            task_id='finishing_pipeline',
            python_callable=dummy_callable,
            op_kwargs={'action': 'finishing'},
        )
        
        (
            start
            >> load_netsuite_to_s3_landing
            >> truncate_postgres_staging
            >> load_s3_landing_to_postgres_staging
            >> [
                load_postgres_staging_to_final,
                load_s3_landing_to_lake
            ]
            >> delete_s3_landing
            >> finish
        )
            
    return dag

if len(config.RECORD_TYPES) > 5:
    raise ValueError('Exceeds NetSuite SuiteScript usage governance limit')

for i, type in enumerate(config.RECORD_TYPES):
    search_type = type['type']
    search_id = type.get('search_id')
    filter = type.get('filter_expression')
    dag_id = f'netsuite_data_pipeline_{search_type}'
    interval = f'{str(i * 10)} * * * *'

    globals()[dag_id] = create_dag(
        dag_id,
        interval,
        config,
        search_type,
        search_id,
        filter
    )