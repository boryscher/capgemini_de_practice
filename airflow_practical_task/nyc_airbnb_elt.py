from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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


def read_table_postgres():
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM airbnb_listings1')    
    data = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    return data


def find_new_records(ti):
    transformed_data = ti.xcom_pull(task_ids='transofrm_df')
    postgres_data = ti.xcom_pull(task_ids='read_table_postgres')
    df = pd.merge(transformed_data, postgres_data, on=transformed_data.columns.tolist(), how="outer", indicator=True
              ).query('_merge=="left_only"')
    df = df.drop(columns=['_merge'])
    return df

def check_file(sql_file):
    try:
        with open(sql_file, 'w') as file:
            file.close()
    except FileNotFoundError:
        pass


def generate_sql(ti, sql_file):
    df = ti.xcom_pull(task_ids='find_new_records')
    for _, row in df.iterrows():
        with open(sql_file, 'a') as file:
            insert_row_raw = f"""INSERT INTO airbnb_listings1 VALUES ({row['id']}, {row['name']}, {row['host_id']}, {row['host_name']}, {row['neighbourhood_group']}, {row['neighbourhood']}),
            {row['latitude']}, {row['longitude']}, {row['room_type']}, {row['price']}, {row['minimum_nights']}, {row['number_of_reviews']}, {row['last_review']}, {row['reviews_per_month']},
            {row['calculated_host_listings_count']}, {row['availability_365']})\n"""
            insert_row = insert_row_raw.replace("'", "\'").replace('"', '\"')
            file.write(insert_row)
    

with DAG(
    dag_id='nyc_airbnb_etl', 
    description='dag for NY AIRBNB ETL', 
    schedule='@daily',
    start_date=datetime(2024, 8, 31),
    default_args=default_args,
    template_searchpath='/redshift/sql/public/flow/',
    params={
        'input_file': Param('/home/borys/Desktop/AB_NYC_2019.csv', type='string'),
        'output_file': Param('/home/borys/Desktop/AB_NYC_2019_ETL.csv', type='string'),
        'sql_file': Param('/home/borys/Desktop/input.sql', type='string')
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
            CREATE TABLE IF NOT EXISTS airbnb_listings1 (
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

    task5 = PythonOperator(
        task_id='read_table_postgres',
        python_callable=read_table_postgres
    )


    task6 = PythonOperator(
        task_id='find_new_records',
        python_callable=find_new_records
    )


    task7 = PythonOperator(
        task_id='check_file',
        python_callable=check_file,
        op_kwargs={'sql_file': dag.params['sql_file']}
    )

    task8 = PythonOperator(
        task_id='generate_sql',
        python_callable=generate_sql,
        op_kwargs={'sql_file': dag.params['sql_file']}
    )


    task9 = SQLExecuteQueryOperator(
        task_id='insert_into_table',
        conn_id='postgres_localhost',
        sql=f"{dag.params['sql_file']}"
    )

    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8 >> task9
