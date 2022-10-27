import requests
reposta = requests.get('https://olinda.bcb.gov.br/olinda/servico/Informes_Agencias/versao/v1/odata/Agencias?$format=json')

resposa_json = reposta.json()
agencias_belo_horizonte = []

for dados_agencia in resposa_json["value"]:
    if dados_agencia["Municipio"] == 'BELO HORIZONTE':
        agencias_belo_horizonte.append(dados_agencia)
        print(dados_agencia)
        
  len(agencias_belo_horizonte)
