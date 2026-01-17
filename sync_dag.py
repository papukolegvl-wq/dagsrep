from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def sync_users_to_payments():
    # 1. Получаем ID всех пользователей из Postgres
    pg_hook = PostgresHook(postgres_conn_id='postgres_local')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    
    pg_cursor.execute("SELECT id FROM test_users")
    user_ids = [row[0] for row in pg_cursor.fetchall()]
    print(f"Найдено {len(user_ids)} пользователей в Postgres")

    if not user_ids:
        print("Пользователи не найдены, выходим.")
        return

    # 2. Подключаемся к Oracle и добавляем платежи
    # Используем провайдер oracledb (thin mode)
    oracle_hook = OracleHook(oracle_conn_id='oracle_local')
    
    # Генерируем случайные платежи для каждого пользователя
    insert_sql = "INSERT INTO SYSTEM.PAYMENTS (user_id, amount, status) VALUES (:1, :2, :3)"
    
    data_to_insert = []
    for uid in user_ids:
        amount = round(random.uniform(100.0, 5000.0), 2)
        status = random.choice(['SUCCESS', 'SUCCESS', 'SUCCESS', 'FAILED']) # Чаще успех
        data_to_insert.append((uid, amount, status))

    # Выполняем вставку
    oracle_hook.run(insert_sql, parameters=data_to_insert)
    print(f"Успешно добавлено {len(data_to_insert)} платежей в Oracle")

with DAG(
    'sync_pg_users_to_oracle_payments',
    default_args=default_args,
    description='Берет юзеров из PG и создает им платежи в Oracle',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    sync_task = PythonOperator(
        task_id='generate_payments_for_pg_users',
        python_callable=sync_users_to_payments,
    )
