from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pandas as pd

default_args = {
    "owner": "admin",
    "retries": 1
}

def read_file(input_file):
    return pd.read_csv(input_file)


def transofrm_df(ti):
    df = ti.xcom_pull(task_ids='read_file')
    new_df = df[df['price'] > 0].copy()
    new_df['last_review'] = pd.to_datetime(new_df['last_review'])
    new_df['last_review'] = new_df['last_review'].fillna(new_df.groupby('neighbourhood_group')['last_review'].transform('min'))
    new_df['reviews_per_month'] = new_df['reviews_per_month'].fillna(0)
    new_df = new_df[~(new_df['latitude'].isnull()) & ~(new_df['longitude'].isnull())]
    return new_df


def save_file(output_file, ti):
    df = ti.xcom_pull(task_ids='transofrm_df')
    df.to_csv(output_file, index=False)



with DAG(
    dag_id='nyc_airbnb_etl', 
    description='dag for NY AIRBNB ETL', 
    schedule='@daily',
    start_date=datetime(2024, 8, 31),
    default_args=default_args,
    params={
        'input_file': Param('/opt/airflow/input/AB_NYC_2019.csv', type='string'),
        'output_file': Param('/opt/airflow/output/AB_NYC_2019_ETL.csv', type='string')
    }
    ) as dag:
    
    task1 = PythonOperator(
        task_id='read_file',
        python_callable=read_file,
        op_kwargs={'input_file': dag.params['input_file']}
    )

    task2 = PythonOperator(
        task_id='transofrm_df',
        python_callable=transofrm_df
    )

    task3 = PythonOperator(
        task_id='save_file',
        python_callable=save_file,
        op_kwargs={'output_file': dag.params['output_file']}
    )


    task4 = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS airbnb_listings (
                id SERIAL PRIMARY KEY,
                name TEXT,
                host_id INTEGER,
                host_name TEXT,neighbourhood_group TEXT,
                neighbourhood TEXT,
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6),
                room_type TEXT,
                price INTEGER,
                minimum_nights INTEGER,
                number_of_reviews INTEGER,
                last_review DATE,
                reviews_per_month DECIMAL(3,2),
                calculated_host_listings_count INTEGER,
                availability_365 INTEGER
                );
            """
    )



    task1 >> task2 >> task3 >> task4

