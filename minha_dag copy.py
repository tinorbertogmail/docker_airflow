from airflow.utils.dates import days_ago
from airflow.models import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator


def ola_mundo():
    print ("----->>>> Eu sou a primeira DAG   <<<------")


with DAG('minha_dag',  start_date= days_ago(1), schedule_interval='@daily', catchup=False) as dag:
    imprimir_ola_mundo = PythonOperator(
        task_id = 'imprimir_ola_mundo',
        python_callable = ola_mundo
    )

imprimir_ola_mundo
