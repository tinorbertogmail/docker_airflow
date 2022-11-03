from airflow.utils.dates import days_ago
from airflow.models import DAG
from datetime import datetime
import requests
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.http_sensor import HttpSensor


def ola_mundo():
    print ("----->>>> Eu sou a DAG de agencia bancariaaaa   <<<------")


def salvar_dados_agencia(**context):
    agencias_belo_horizonte = context['ti'].xcom_pull(key="dados_agencias")
    print("--->> Numero de agências {}".format(str(len(agencias_belo_horizonte))))


def obter_dados_agencia(**context):
    reposta = requests.get('https://olinda.bcb.gov.br/olinda/servico/Informes_Agencias/versao/v1/odata/Agencias?$format=json')
    resposa_json = reposta.json()
    agencias_belo_horizonte = []

    for dados_agencia in resposa_json["value"]:
        if dados_agencia["Municipio"] == 'BELO HORIZONTE':
            agencias_belo_horizonte.append(dados_agencia)
            print(dados_agencia)
    context['ti'].xcom_push(key="dados_agencias", value=agencias_belo_horizonte)        
    print("--->> Numero de agências {}".format(str(len(agencias_belo_horizonte))))

with DAG('agencia_bancaria_dag',  start_date= days_ago(1), schedule_interval='@daily', catchup=False) as dag:
    
    begin = DummyOperator(
        task_id='begin'
    )
    
    imprimir_ola_mundo = PythonOperator(
        task_id = 'imprimir_ola_mundo',
        python_callable = ola_mundo
    )

    verificacao_de_endpoint = HttpSensor(
        task_id='verificacao_de_endpoint',
        http_conn_id='http_olinda_api',
        endpoint='servico/Informes_Agencias/versao/v1/odata/Agencias?$format=json',
        method='GET',
        request_params={},
        response_check=lambda response: response.status_code == 200,
        extra_options={"timeout":60},
        poke_interval=30
    )

    obter_dados_agencia = PythonOperator(
        task_id = 'oter_dados_agencia',
        python_callable = obter_dados_agencia,
        provide_context=True
    )

    salvar_dados_agencia = PythonOperator(
        task_id = 'salvar_dados_agencia',
        python_callable = salvar_dados_agencia
    )

    end = DummyOperator(
        task_id='end'
    )


begin >> verificacao_de_endpoint >> obter_dados_agencia >> salvar_dados_agencia >> end
