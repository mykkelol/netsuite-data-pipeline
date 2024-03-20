from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator

from dags_config import Config as config
from transformation import transform_staged_data
from custom_operators import (
    NetSuiteToS3Operator,
    S3ToPostgresTransferOperator
)

def dummy_callable(action):
    return (
        f'{datetime.now()}: {action} '
        f'NetSuite GL posting transactions data pipeline'
    )

def get_bucket_key(base = 'netsuite_extracts', extension = '.csv'):
    return f'{base}_{{{{ ds_nodash }}}}{extension}'

def create_dag(
    dag_id,
    description,
    interval,
    config,
    search_type,
    search_id,
    filter,
    subsearches
):
    with DAG(
        dag_id=dag_id,
        description=description,
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

        with TaskGroup(group_id='load_netsuite_to_s3_landing') as load_netsuite_to_s3_landing:
            tasks = [
                NetSuiteToS3Operator(
                    task_id=f'extract_{type}',
                    search_types=config.SUPPORTED_RECORD_TYPES,
                    search_type=type,
                    search_id=id,
                    conn_id=config.S3_CONN_ID,
                    bucket_name=config.LANDING_BUCKET,
                    filename=get_bucket_key(extension=''),
                    filter_expression=filter if i == 0 else subfilter,
                    columns=None,
                    dag=dag
                ) for i, (type, id, subfilter) in enumerate(subsearches)
            ]

        truncate_postgres_staging = PostgresOperator(
            task_id=f'truncate_postgres_tables',
            postgres_conn_id=config.POSTGRES_CONN_ID,
            sql='truncate_postgres_staging.sql',
            params={'table_id': f'stage_{search_id}'}
        )

        with TaskGroup(group_id='load_s3_landing_to_postgres_staging') as load_s3_landing_to_postgres_staging:
            tasks = [
                S3ToPostgresTransferOperator(
                    task_id=f'load_to_postgres_stage_{type}',
                    aws_conn_id=config.S3_CONN_ID,
                    s3_key=get_bucket_key(extension='.csv'),
                    s3_bucket_name=config.LANDING_BUCKET,
                    postgres_conn_id=config.POSTGRES_CONN_ID,
                    postgres_table=f'stage_{id}',
                    dag=dag
                ) for (type, id, subfilter) in subsearches
            ]

        transform_postgres_staging = PythonOperator(
            task_id=f'transform_postgres_staging',
            python_callable=transform_staged_data,
            dag=dag,
            op_kwargs={
                'postgres_conn_id': config.POSTGRES_CONN_ID,
                'postgres_table': f'stage_{search_id}',
            }
        )

        load_postgres_staging_to_final = PostgresOperator(
            task_id=f'load_postgres_final_{search_type}',
            postgres_conn_id=config.POSTGRES_CONN_ID,
            sql='load_postgres_staging_to_final.sql',
            params={'search_id': search_id}
        )

        load_s3_landing_to_lake = S3CopyObjectOperator(
            task_id=f'load_s3_lake_{search_type}',
            aws_conn_id=config.S3_CONN_ID,
            source_bucket_key=f'S3://{config.LANDING_BUCKET}/{get_bucket_key()}',
            dest_bucket_key=f'S3://{config.LAKE_BUCKET}/{get_bucket_key()}',
            dag=dag,
        )

        delete_s3_landing = S3DeleteObjectsOperator(
            task_id=f'delete_s3_landing_{search_type}',
            aws_conn_id=config.S3_CONN_ID,
            bucket=config.LANDING_BUCKET,
            keys=get_bucket_key(extension='.csv'),
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
            >> transform_postgres_staging
            >> [
                load_postgres_staging_to_final,
                load_s3_landing_to_lake,
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
    subsearches = type.get('subsearches')
    filter = type.get('filter')

    dag_id = f'netsuite_data_pipeline_{search_type}'
    description = f'Data Pipeline for NetSuite {search_type}'
    interval = f'{str(i * 5)} * * * *'

    globals()[dag_id] = create_dag(
        dag_id,
        description,
        interval,
        config,
        search_type,
        search_id,
        filter,
        subsearches
    )