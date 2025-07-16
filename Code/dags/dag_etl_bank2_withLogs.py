from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
import time

FOLDER_PATH = '/opt/airflow/datasets/'
TMP_PATH = '/opt/airflow/tmp/'

DB_CONN_STR = "postgresql+psycopg2://airflow:airflow@postgres:5432/ETL_bank"


def init_logging_system(engine):

    with engine.connect() as conn:

        with conn.begin():

            conn.execute(text("CREATE SCHEMA IF NOT EXISTS logs;"))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS logs.etl_logs (
                    log_id SERIAL PRIMARY KEY,
                    dag_id VARCHAR(100) NOT NULL,
                    task_id VARCHAR(100) NOT NULL,
                    process_name VARCHAR(100) NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status VARCHAR(20),
                    rows_affected INTEGER,
                    message TEXT,
                    duration INTERVAL GENERATED ALWAYS AS (end_time - start_time) STORED
                );
            """))


def log_etl_event(engine, dag_id, task_id, process_name, start_time=None,
                 end_time=None, status=None, rows_affected=None, message=None):
    try:
        with engine.connect() as conn:
            with conn.begin():
                if start_time and not end_time:
                    conn.execute(text("""
                        INSERT INTO logs.etl_logs 
                        (dag_id, task_id, process_name, start_time, status)
                        VALUES (:dag_id, :task_id, :process_name, :start_time, 'started')
                    """), {
                        'dag_id': dag_id,
                        'task_id': task_id,
                        'process_name': process_name,
                        'start_time': start_time
                    })
                elif end_time:
                    conn.execute(text("""
                        UPDATE logs.etl_logs 
                        SET end_time = :end_time, 
                            status = :status,
                            rows_affected = :rows_affected,
                            message = :message
                        WHERE log_id = (
                            SELECT log_id 
                            FROM logs.etl_logs 
                            WHERE dag_id = :dag_id 
                            AND task_id = :task_id
                            AND process_name = :process_name
                            AND end_time IS NULL
                            ORDER BY start_time DESC
                            LIMIT 1
                        )
                    """), {
                        'dag_id': dag_id,
                        'task_id': task_id,
                        'process_name': process_name,
                        'end_time': end_time,
                        'status': status,
                        'rows_affected': rows_affected,
                        'message': message
                    })
    except Exception as e:
        print(f"Ошибка при логировании: {e}")


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
        rows_count = len(df)

        log_etl_event(
            engine=engine,
            dag_id='dag_etl_flow_bank_v0',
            task_id='load_task',
            process_name=f'load_{table_name}',
            start_time=datetime.now()
        )

        time.sleep(5)

        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace',
            index=False
        )

        log_etl_event(
            engine=engine,
            dag_id='dag_etl_flow_bank_v0',
            task_id='load_task',
            process_name=f'load_{table_name}',
            end_time=datetime.now(),
            status='completed',
            rows_affected=rows_count,
            message=f'Successfully loaded {rows_count} rows to {table_name}'
        )

        print(f"Loaded: {table_name}")
        return 1
    except Exception as e:
        log_etl_event(
            engine=engine,
            dag_id='dag_etl_flow_bank_v0',
            task_id='load_task',
            process_name=f'load_{table_name}',
            end_time=datetime.now(),
            status='failed',
            message=str(e)
        )
        print(f"Error loading {name}: {e}")
        return 0


def extract_task(**kwargs):
    ti = kwargs['ti']
    engine = create_engine(DB_CONN_STR)

    init_logging_system(engine)

    log_etl_event(
        engine=engine,
        dag_id=kwargs['dag'].dag_id,
        task_id=kwargs['task'].task_id,
        process_name='extract',
        start_time=datetime.now()
    )

    try:
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

        log_etl_event(
            engine=engine,
            dag_id=kwargs['dag'].dag_id,
            task_id=kwargs['task'].task_id,
            process_name='extract',
            end_time=datetime.now(),
            status='completed',
            rows_affected=len(files),
            message=f'Extracted {len(files)} files'
        )

        print(f"Extracted files: {files}")
        return files

    except Exception as e:
        log_etl_event(
            engine=engine,
            dag_id=kwargs['dag'].dag_id,
            task_id=kwargs['task'].task_id,
            process_name='extract',
            end_time=datetime.now(),
            status='failed',
            message=str(e)
        )
        raise e
    finally:
        engine.dispose()


def transform_task(**kwargs):
    ti = kwargs['ti']
    engine = create_engine(DB_CONN_STR)

    log_etl_event(
        engine=engine,
        dag_id=kwargs['dag'].dag_id,
        task_id=kwargs['task'].task_id,
        process_name='transform',
        start_time=datetime.now()
    )

    try:
        files = ti.xcom_pull(task_ids='extract_task')
        transformed_files = []

        for parquet_file in files:
            df_name = os.path.basename(parquet_file).split('.')[0]
            df = pd.read_parquet(parquet_file)
            df = transform_df_to_sql(df_name, df)

            transformed_file = parquet_file.replace('.parquet', '_clean.parquet')
            df.to_parquet(transformed_file)
            transformed_files.append(transformed_file)

        log_etl_event(
            engine=engine,
            dag_id=kwargs['dag'].dag_id,
            task_id=kwargs['task'].task_id,
            process_name='transform',
            end_time=datetime.now(),
            status='completed',
            rows_affected=len(transformed_files),
            message=f'Transformed {len(transformed_files)} files'
        )

        print(f"Transformed files: {transformed_files}")
        return transformed_files

    except Exception as e:
        log_etl_event(
            engine=engine,
            dag_id=kwargs['dag'].dag_id,
            task_id=kwargs['task'].task_id,
            process_name='transform',
            end_time=datetime.now(),
            status='failed',
            message=str(e)
        )
        raise e
    finally:
        engine.dispose()


def load_task(**kwargs):
    ti = kwargs['ti']
    engine = create_engine(DB_CONN_STR)

    log_etl_event(
        engine=engine,
        dag_id=kwargs['dag'].dag_id,
        task_id=kwargs['task'].task_id,
        process_name='load',
        start_time=datetime.now()
    )

    try:
        files = ti.xcom_pull(task_ids='transform_task')
        total_rows = 0

        for parquet_file in files:
            df_name = os.path.basename(parquet_file).split('_clean')[0]
            df = pd.read_parquet(parquet_file)
            result = load_df_to_sql(df_name, df, engine)
            total_rows += len(df)

        log_etl_event(
            engine=engine,
            dag_id=kwargs['dag'].dag_id,
            task_id=kwargs['task'].task_id,
            process_name='load',
            end_time=datetime.now(),
            status='completed',
            rows_affected=total_rows,
            message=f'Loaded {total_rows} total rows to database'
        )

        print("All files loaded successfully.")

    except Exception as e:
        log_etl_event(
            engine=engine,
            dag_id=kwargs['dag'].dag_id,
            task_id=kwargs['task'].task_id,
            process_name='load',
            end_time=datetime.now(),
            status='failed',
            message=str(e)
        )
        raise e
    finally:
        engine.dispose()


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        dag_id='dag_etl_with_logs_v3',
        default_args=default_args,
        description='ETL DAG с системой логирования',
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