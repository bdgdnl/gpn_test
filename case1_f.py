from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from pathlib import Path
import psycopg2
import csv

file_path = Path('/opt/airflow/files/t_employee.csv')
connect_param = "dbname=gpn user=gpadmin password=eJ63cH4j host=192.168.77.17"
tz = "SET TIME ZONE 'Asia/Yekaterinburg';"

args = {
    'owner': 'reewos',
    'start_date': datetime(2024,4,22),
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
}

def update_csv():
    with open(file_path, mode='r', newline='') as file:
        reader = csv.reader(file)
        rows = list(reader)

    update_rows = []
    for row in rows:
        update_row = row[0].split(';')
        if update_row[-1] == '1':
            update_row[-1] = '0'
        update_rows.append([';'.join(update_row)])

    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(update_rows)

def truncate_and_load_current_data():
    conn = psycopg2.connect(connect_param)
    cur = conn.cursor()
    cur.execute(tz)
    cur.execute('truncate table t_employee_current;')

    with open(file_path, 'r') as file:
        reader = csv.reader(file, delimiter=';')
        next(reader)  # Пропустить заголовок
        for row in reader:
            department = row[0]
            position = row[1]
            employee_id = row[2]
            full_name = row[3]
            birth_date = datetime.strptime(row[4], '%d.%m.%Y').date()
            address = row[5]
            phone1 = row[6]
            phone2 = row[7]
            month = row[8]
            worked_hours = int(row[9])
            need_update = bool(int(row[10]))
            cur.execute("""
                INSERT INTO t_employee_current (employee_id, full_name, department, position, birth_date,
                    address, phone1, phone2, month, worked_hours, need_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (employee_id, full_name, department, position, birth_date, address, phone1,
                    phone2, month, worked_hours, need_update))   
    conn.commit()
    cur.close()
    conn.close()


def insert_rows():
    conn = psycopg2.connect(connect_param)
    cur = conn.cursor()
    cur.execute(tz)
    cur.execute("""
        SELECT c.*
        FROM t_employee_current c
        LEFT JOIN t_employee_history h
        ON c.employee_id = h.employee_id
        WHERE h.employee_id IS null
    """)

    if cur.rowcount > 0:
        cur.execute("""
            INSERT INTO t_employee_history (employee_id, full_name, department, position, birth_date, address, phone1, phone2, month, worked_hours, action_flag, start_date, end_date)
            SELECT c.employee_id, c.full_name, c.department, c.position, c.birth_date, c.address, c.phone1, c.phone2, c.month, c.worked_hours, 'I', NOW(), '9999-12-31'
            FROM t_employee_current c
            LEFT JOIN t_employee_history h ON c.employee_id = h.employee_id
            WHERE h.employee_id IS null;
        """)     
    conn.commit()
    cur.close()
    conn.close()

def update_rows():
    conn = psycopg2.connect(connect_param)
    cur = conn.cursor()
    cur.execute(tz)
    cur.execute("""
        SELECT DISTINCT c.*
        FROM t_employee_current c
        JOIN t_employee_history h
        ON c.employee_id = h.employee_id
        WHERE c.need_update = true  
        AND (c.full_name != h.full_name
        OR c.department != h.department
        OR c.position != h.position
        OR c.birth_date != h.birth_date
        OR c.address != h.address
        OR c.phone1 != h.phone1
        OR c.phone2 != h.phone2
        OR c.month != h.month
        OR c.worked_hours != h.worked_hours);
    """)

    if cur.rowcount > 0:
        cur.execute("""
            INSERT INTO t_employee_history (department, position, employee_id, full_name, birth_date,
                                        address, phone1, phone2, month, worked_hours,
                                        action_flag, start_date, end_date)
            SELECT DISTINCT c.department, c.position, c.employee_id, c.full_name, c.birth_date,
                    c.address, c.phone1, c.phone2, c.month, c.worked_hours,
                    'U', CAST(NOW() AS TIMESTAMP), TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
            FROM t_employee_current c
            JOIN t_employee_history h
            ON c.employee_id = h.employee_id
            WHERE c.full_name != h.full_name
            OR c.department != h.department
            OR c.position != h.position
            OR c.birth_date != h.birth_date
            OR c.address != h.address
            OR c.phone1 != h.phone1
            OR c.phone2 != h.phone2
            OR c.month != h.month
            OR c.worked_hours != h.worked_hours;
        """)    

        cur.execute("""
            UPDATE t_employee_history h
            SET end_date = NOW()
            FROM t_employee_current c
            WHERE c.employee_id = h.employee_id
            AND (
                c.full_name != h.full_name
                OR c.department != h.department
                OR c.position != h.position
                OR c.birth_date != h.birth_date
                OR c.address != h.address
                OR c.phone1 != h.phone1
                OR c.phone2 != h.phone2
                OR c.month != h.month
                OR c.worked_hours != h.worked_hours
            );
        """)  

        update_csv()

    conn.commit()
    cur.close()
    conn.close()

def delete_rows():
    conn = psycopg2.connect(connect_param)
    cur = conn.cursor()
    cur.execute("SET TIME ZONE 'Asia/Yekaterinburg';")
    cur.execute("""
        SELECT h.*
        FROM t_employee_history h
        LEFT JOIN t_employee_current c
        ON c.employee_id = h.employee_id
        WHERE c.employee_id IS null
        and h.end_date = '9999-12-31'
    """)

    if cur.rowcount > 0:
        cur.execute("""
            INSERT INTO t_employee_history (department, position, employee_id, full_name,
                                            birth_date, address, phone1, phone2, month,
                                            worked_hours, action_flag, start_date, end_date)
            SELECT h.department, h.position, h.employee_id, h.full_name,
                    h.birth_date, h.address, h.phone1, h.phone2, h.month,
                    h.worked_hours, 'D', NOW(), NOW()
            FROM t_employee_history h
            LEFT JOIN t_employee_current c
            ON c.employee_id = h.employee_id
            WHERE c.employee_id IS null
            and h.end_date = '9999-12-31'
        """)    

        cur.execute("""
            UPDATE t_employee_history 
            SET end_date = NOW()
            WHERE employee_id NOT IN (SELECT employee_id FROM t_employee_current)
            AND action_flag != 'D'
            AND end_date = '9999-12-31';
        """)  

    conn.commit()
    cur.close()
    conn.close()

def load_history_data():
    insert_rows()
    update_rows()
    delete_rows()

with DAG('load_employee_data', default_args=args, description='Load employee data into Greenplum', schedule_interval='@monthly') as dag:

    load_current_data_task = PythonOperator(
        task_id='truncate_and_load_current_data',
        python_callable=truncate_and_load_current_data,
        dag=dag
    )

    load_history_data_task = PythonOperator(
    task_id='load_history_data',
    python_callable=load_history_data,
    dag=dag
    )

    load_current_data_task >> load_history_data_task