from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor

import requests

'''
Código de exemplo de um DAG
'''

default_args = {
    "owner": "yhaeffner",
    "start_date": days_ago(1)
}

def obter_variacao(id_da_criptomoeda="bitcoin", sigla_fiat="brl", **context):
    r = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={id_da_criptomoeda}&vs_currencies={sigla_fiat}&include_market_cap=false&include_24hr_vol=false&include_24hr_change=true&include_last_updated_at=false")
    json_response = r.json()
    if id_da_criptomoeda in json_response:
        key_variacao = f"{sigla_fiat}_24h_change"
        context['ti'].xcom_push(key=f"variacao_{id_da_criptomoeda}", value=json_response[id_da_criptomoeda][key_variacao])
    else:
        raise ValueError(f"Falha ao consumir dados da API para criptomoeda com ID ({id_da_criptomoeda}) e fiat ({sigla_fiat})")

def comparar_variacao(lista_de_criptos = [], **context):
    maior_variacao = -99999
    criptomoeda_de_maior_variacao = None
    for criptomoeda in lista_de_criptos:
        variacao = context['ti'].xcom_pull(key=f"variacao_{criptomoeda}", task_ids=[f"obter_variacao_{criptomoeda}"])[0]
        print(f"Criptomoeda: {criptomoeda}\t Variação: {variacao}")
        if variacao > maior_variacao:
            maior_variacao = variacao
            criptomoeda_de_maior_variacao = criptomoeda
    print(f"({criptomoeda_de_maior_variacao}) foi a criptomoeda que teve a maior variação nas últimas 24 horas, com variação de ({round(maior_variacao, 2)}%)")

with DAG('diario_de_criptos_DAG', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    begin = DummyOperator(
        task_id='begin'
    )

    verificacao_de_endpoint = HttpSensor(
        task_id='verificacao_de_endpoint',
        http_conn_id='http_coingecko_api',
        endpoint="/api/v3/ping",
        method='GET',
        request_params={},
        response_check=lambda response: "To the Moon" in response.json()['gecko_says'],
        extra_options={"timeout":60},
        poke_interval=30
    )

    obter_variacao_bitcoin = PythonOperator(
        task_id='obter_variacao_bitcoin',
        python_callable=obter_variacao,
        provide_context=True,
        op_kwargs={"id_da_criptomoeda":"bitcoin", "sigla_fiat":"brl"}
    )

    obter_variacao_ethereum = PythonOperator(
        task_id='obter_variacao_ethereum',
        python_callable=obter_variacao,
        provide_context=True,
        op_kwargs={"id_da_criptomoeda":"ethereum", "sigla_fiat":"brl"}
    )

    obter_variacao_doge = PythonOperator(
        task_id='obter_variacao_dogecoin',
        python_callable=obter_variacao,
        provide_context=True,
        op_kwargs={"id_da_criptomoeda":"dogecoin", "sigla_fiat":"brl"}
    )

    escolher_criptomoeda_de_maior_variacao = PythonOperator(
        task_id='escolher_criptomoeda_de_maior_variacao',
        provide_context=True,
        python_callable=comparar_variacao,
        op_kwargs={"lista_de_criptos": ["bitcoin", "ethereum", "dogecoin"]}
    )

    end = DummyOperator(
        task_id='end'
    )

    begin >> verificacao_de_endpoint >> [obter_variacao_bitcoin, obter_variacao_ethereum, obter_variacao_doge] >> escolher_criptomoeda_de_maior_variacao >> end