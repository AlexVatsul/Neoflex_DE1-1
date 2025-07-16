from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

FOLDER_PATH = '/opt/airflow/datasets/'
TMP_PATH = '/opt/airflow/tmp/'

DB_CONN_STR = "postgresql+psycopg2://airflow:airflow@postgres:5432/ETL_bank"


def transform_df_to_sql(name: str, df: pd.DataFrame) -> pd.DataFrame:
    if name == 'df_ft_balance_f':
        df['on_date'] = pd.to_datetime(df['on_date'], format='%d.%m.%Y')
    elif name == 'df_ft_posting_f':
        df['oper_date'] = pd.to_datetime(df['oper_date'], format='%d-%m-%Y')
    elif name == 'df_md_account_d':
        df['data_actual_date'] = pd.to_datetime(df['data_actual_date'], format='%Y-%m-%d')
        df['data_actual_end_date'] = pd.to_datetime(df['data_actual_end_date'], format='%Y-%m-%d')
        df['currency_code'] = df['currency_code'].astype(str)
    elif name == 'df_md_currency_d':
        df['data_actual_date'] = pd.to_datetime(df['data_actual_date'], format='%Y-%m-%d')
        df['data_actual_end_date'] = pd.to_datetime(df['data_actual_end_date'], format='%Y-%m-%d')
        df['currency_code'] = df['currency_code'].astype(float).astype('Int64')
        df['currency_code'] = (
            df['currency_code']
            .astype(str)
            .str[:3]
            .str.pad(width=3, side='left', fillchar='0')
        )
    elif name == 'df_md_exchange_rate_d':
        df['data_actual_date'] = pd.to_datetime(df['data_actual_date'], format='%Y-%m-%d')
        df['data_actual_end_date'] = pd.to_datetime(df['data_actual_end_date'], format='%Y-%m-%d')
        df['code_iso_num'] = df['code_iso_num'].astype(str)
    elif name == 'df_md_ledger_account_s':
        df['start_date'] = pd.to_datetime(df['start_date'], format='%Y-%m-%d')
        df['end_date'] = pd.to_datetime(df['end_date'], format='%Y-%m-%d')
    return df


def load_df_to_sql(name: str, df: pd.DataFrame, engine, schema: str = 'ds') -> int:
    try:
        table_name = name.replace('df_', '')
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace',
            index=False
        )
        print(f"Loaded: {table_name}")
        return 1
    except Exception as e:
        print(f"Error loading {name}: {e}")
        return 0


def extract_task(**kwargs):
    os.makedirs(TMP_PATH, exist_ok=True)

    files = []
    for file_name in os.listdir(FOLDER_PATH):
        if file_name.endswith('.csv'):
            file_path = os.path.join(FOLDER_PATH, file_name)
            df_name = 'df_' + file_name.split('.')[0]

            if file_name == 'md_currency_d.csv':
                df = pd.read_csv(file_path, encoding='cp1252', sep=';')
            else:
                df = pd.read_csv(file_path, sep=';')

            df.columns = df.columns.str.lower()

            parquet_file = os.path.join(TMP_PATH, f"{df_name}.parquet")
            df.to_parquet(parquet_file)
            files.append(parquet_file)

    print(f"Extracted files: {files}")
    return files


def transform_task(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(task_ids='extract_task')

    transformed_files = []
    for parquet_file in files:
        df_name = os.path.basename(parquet_file).split('.')[0]
        df = pd.read_parquet(parquet_file)
        df = transform_df_to_sql(df_name, df)

        transformed_file = parquet_file.replace('.parquet', '_clean.parquet')
        df.to_parquet(transformed_file)
        transformed_files.append(transformed_file)

    print(f"Transformed files: {transformed_files}")
    return transformed_files


def load_task(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(task_ids='transform_task')

    engine = create_engine(DB_CONN_STR)

    for parquet_file in files:
        df_name = os.path.basename(parquet_file).split('_clean')[0]
        df = pd.read_parquet(parquet_file)
        load_df_to_sql(df_name, df, engine)

    engine.dispose()
    print("All files loaded successfully.")


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_etl_flow_bank_v0',
    default_args=default_args,
    description='Чистый ETL DAG: Extract -> Transform -> Load',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_task',
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id='transform_task',
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id='load_task',
        python_callable=load_task
    )

    extract >> transform >> load
