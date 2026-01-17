
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import csv
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def export_users_to_file():
    # Получаем соединение с PostgreSQL
    # Мы создали коннекшн postgres_local с помощью команды airflow connections add
    hook = PostgresHook(postgres_conn_id='postgres_local')
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    # Выполняем запрос
    cursor.execute("SELECT * FROM test_users")
    rows = cursor.fetchall()
    
    # Получаем названия колонок
    colnames = [desc[0] for desc in cursor.description]
    
    # Определяем путь к папке (В WSL /mnt/c/ это диск C: в Windows)
    output_dir = "/mnt/c/Хранилище"
    
    # Создаем папку если её нет
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Формируем имя файла с текущей датой
    filename = f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(output_dir, filename)
    
    # Записываем в CSV
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(colnames)
        writer.writerows(rows)
        
    print(f"Успешно выгружено {len(rows)} строк в файл: {file_path}")

with DAG(
    'export_users_to_disk_c',
    default_args=default_args,
    description='Выгрузка пользователей из Postgres на Диск C',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, # Запуск вручную, или поставьте '@daily'
    catchup=False,
) as dag:

    export_task = PythonOperator(
        task_id='export_postgres_to_local_file',
        python_callable=export_users_to_file,
    )
