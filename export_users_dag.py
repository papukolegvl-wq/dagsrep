from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime, timedelta
import os
import csv

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Папка экспорта на диске C Windows
OUTPUT_DIR = "/mnt/c/Хранилище"

def export_to_storage():
    """Задача: Выгрузка данных из PG и Oracle в CSV файлы в Хранилище"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Создана папка: {OUTPUT_DIR}")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 1. Выгрузка из Postgres (Users)
    print("Экспорт из Postgres...")
    pg_hook = PostgresHook(postgres_conn_id='postgres_local')
    pg_records = pg_hook.get_records("SELECT * FROM test_users")
    pg_file = os.path.join(OUTPUT_DIR, f"pg_users_{timestamp}.csv")
    
    with open(pg_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ID', 'USERNAME', 'EMAIL', 'CREATED_AT'])
        writer.writerows(pg_records)
    print(f"Данные Postgres сохранены: {pg_file}")

    # 2. Выгрузка из Oracle (Payments)
    print("Экспорт из Oracle...")
    oracle_hook = OracleHook(oracle_conn_id='oracle_local')
    ora_records = oracle_hook.get_records("SELECT * FROM SYSTEM.PAYMENTS")
    ora_file = os.path.join(OUTPUT_DIR, f"oracle_payments_{timestamp}.csv")
    
    with open(ora_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['PAYMENT_ID', 'USER_ID', 'AMOUNT', 'PAYMENT_DATE', 'STATUS'])
        writer.writerows(ora_records)
    print(f"Данные Oracle сохранены: {ora_file}")

with DAG(
    'export_users_to_disk_c',
    default_args=default_args,
    description='Выгрузка пользователей из Postgres и платежей из Oracle на Диск C',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task_export = PythonOperator(
        task_id='export_databases_to_csv',
        python_callable=export_to_storage,
    )
