from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import psycopg2

# Параметры подключения к серверу Greenplum
greenplum_conn = {
    "host": "192.168.77.17",
    "database": "gpn",
    "user": "gp_user",
    "password": "gpuser123"
}

# Параметры подключения к серверу PostgreSQL
postgres_conn = {
    "host": "192.168.77.6",
    "database": "gpn_pg",
    "user": "pguser",
    "password": "pguser123"
}

args = {
    'owner': 'reewos',
    'start_date': datetime(2024,4,22),
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
}

def extract_and_load():
    postgres_connection = psycopg2.connect(**postgres_conn)
    postgres_cursor = postgres_connection.cursor()

    greenplum_connection = psycopg2.connect(**greenplum_conn)
    greenplum_cursor = greenplum_connection.cursor()

    postgres_cursor.execute("TRUNCATE TABLE t_employee_current")

    greenplum_cursor.execute("SELECT employee_id, full_name, department, position, birth_date, address, phone1, phone2, month, worked_hours FROM t_employee_current")
    rows = greenplum_cursor.fetchall()
    for row in rows:
        postgres_cursor.execute("INSERT INTO t_employee_current (employee_id, full_name, department, position, birth_date, address, phone1, phone2, month, worked_hours) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

    postgres_connection.commit()
    postgres_cursor.close()
    postgres_connection.close()

    greenplum_cursor.close()
    greenplum_connection.close()

with DAG('extract_and_load_values', default_args=args, description='Extract employee data from Greenplum and load into Postgres', schedule_interval=None) as dag:

    extract_and_load_values = PythonOperator(
        task_id='truncate_and_load_current_data',
        python_callable=extract_and_load,
        dag=dag
    )


    extract_and_load_values