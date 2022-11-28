# Projeto docker_airflow

Projeto para exemplo de códigos utilizando o Apache Airflow. 

## Inicializar o Airflow

Pasta da instalação: apache-airflow/

Verificar se o serviço está rodando:
ps -aux | grep airflow

Verificar o containier do doker
docker compose ls

Verificar o docker compose
docker-compose up


Projeto para a aprendizagem do docker e apache airflow. 
Para instalar o apache airflow basta seguir os passos da documentação. Não encontrei nenhum problema .


### Hello Docker
Objetivo: Primeiros códigos com o Apache Airflow
Primeiro código com uma Hello World utilizando o PythonOperator. 
Imprimir em um log uma mensagem executada por uma função utilizando um PythonOperator.
Nome do arquivo: minha_dag copy.py

### Buscar agências bancarias:
Objetivo: Utilizar um operador Http e a variável XCOM
Buscar os dados do serviço https://olinda.bcb.gov.br/olinda/servico/Informes_Agencias/versao/v1/odata/Agencias?$format=json fazer o tratamento e exibir o número de agencias bancarias na cidade de Belo Horizonte
Não esquecer de cadastrar o serviço para 
Nome do arquivo: agencia_bancaria_dag.py

### Buscar criptmoedas:
Objetivo: Utilizar essa código como base para aprendizagem
Buscar os dados de um serviço e determinar qual das criptmoedas tem um  variação maior. 
Utilizar o PythonOperator e HttpSensor para obter os dados do serviço de criptomoedas. Criar uma conexão http_coingecko_api com https://www.coingecko.com/en/api. 

Nome do arquivo: diario_de_criptos_DAG.py 


### Buscar informações sobre o tempo:
Objetivo: Obter os dados de um serviço de dados climáticos e salvar em arquivos csv
Buscar os dados do serviço https://weather.visualcrossing.com/VisualCrossingWebServices/ e salvar em arquivo csv. Utilizar o PythonOperator para criar a tarefa.
Nome do arquivo: buscar_informacoes_tempo.py
