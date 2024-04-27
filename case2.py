from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
import requests
import psycopg2

args = {
    'owner': 'reewos',
    'start_date': datetime(2024,4,22),
    'retries': 3,
    'retry_delay': timedelta(seconds=300)
}

def call_wikipedia_api_method():
    response = requests.get("https://ru.wikipedia.org/w/api.php?action=query&list=allpages&aplimit=500&apfrom=A&format=json")
    return response.json()

def process_wikipedia_api_response(**kwargs):
    response = kwargs['ti'].xcom_pull(task_ids='call_wikipedia_api')
    allpages_data = response['query']['allpages']

    conn = psycopg2.connect("dbname=gpn user=gpadmin password=eJ63cH4j host=192.168.77.17")
    cur = conn.cursor()

    for page in allpages_data:
        cur.execute("INSERT INTO allpages (pageid, ns, title) VALUES (%s, %s, %s)", (page['pageid'], page['ns'], page['title']))

    conn.commit()
    cur.close()
    conn.close()

with DAG('wikipedia_api_etl', description='case2', schedule_interval=None, default_args=args) as dag:
    call_wikipedia_api = PythonOperator(
        task_id='call_wikipedia_api',
        python_callable=call_wikipedia_api_method,
    )

    process_response = PythonOperator(
        task_id='process_wikipedia_api_response',
        python_callable=process_wikipedia_api_response,
        provide_context=True
    )

    call_wikipedia_api >> process_response