from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from dags_config import Config as config
from custom_operators import (
    NetSuiteSearchOperator
)

def dummy_callable(action):
    return f'{datetime.now()}: {action} NetSuite GL posting transactions data pipeline'

def get_netsuite_results(config, type, searchId, dag):
    return NetSuiteSearchOperator(
        task_id=f'extract_netsuite_{type}_{searchId}',
        type=type,
        searchId=searchId,
        dag=dag
    )

def create_dag(dag_id, interval, config, type, searches):
    with DAG(
        dag_id=dag_id,
        description=f'Ingested new GL posting {{type}} from NetSuite',
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

        # netsuite_results = [
        #     get_netsuite_results(config, type, searchId, dag)
        #     for searchId in searches
        # ]

        finish = PythonOperator(
            task_id='finishing_pipeline',
            python_callable=dummy_callable,
            op_kwargs={'action': 'finishing'},
            dag=dag
        )

        start >> finish
    
    return dag

if len(config.TRANSACTION_TYPES) > 5:
    raise ValueError('Exceeds NetSuite SuiteScript usage governance limit')

for i, item in enumerate(config.TRANSACTION_TYPES.items()):
    type, searches = item
    dag_id = f'netsuite_data_pipeline_{type}'
    interval = f'{str(i * 10)} * * * *'

    globals()[dag_id] = create_dag(
        dag_id,
        interval,
        config,
        type,
        searches
    )