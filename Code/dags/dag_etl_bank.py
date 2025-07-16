from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text


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
        table_name = name.replace('df_', '')  # Убираем префикс 'df_'

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


def etl_task(**kwargs):
    folder_path = '/opt/airflow/datasets/'
    file_names = [
        file_name for file_name in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, file_name)) and file_name.endswith('.csv')
    ]

    connection_string = "postgresql+psycopg2://airflow:airflow@postgres:5432/ETL_bank"
    engine = create_engine(connection_string)

    for file_name in file_names:
        df_name = 'df_' + file_name.split('.')[0]
        file_path = os.path.join(folder_path, file_name)

        if file_name == 'md_currency_d.csv':
            df = pd.read_csv(file_path, encoding='cp1252', sep=';')
        else:
            df = pd.read_csv(file_path, sep=';')

        df.columns = df.columns.str.lower()
        df = transform_df_to_sql(df_name, df)

        load_df_to_sql(df_name, df, engine)

    engine.dispose()


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}



with DAG(
    dag_id='dag_etl_mono_bank_v0',
    default_args=default_args,
    description='Простой DAG с PythonOperator',
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Запускать каждый день
    catchup=False,
) as dag:
    task_etl = PythonOperator(
        task_id='etl_task',
        python_callable=etl_task
    )

    task_etl