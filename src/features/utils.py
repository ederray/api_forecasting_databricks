from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import date
from io import StringIO
import requests
import pandas as pd
from bs4 import BeautifulSoup
import urllib3 

spark = SparkSession.builder.appName("API_Forecasting_Utils").getOrCreate()

def import_query(path):

    with open(path) as file:
        return file.read()

def criar_tabela_spark(df: DataFrame) ->DataFrame:
    df = spark.createDataFrame(df)
    return df  


def carregar_tabela_spark(format: str, 
                          path: str,
                         sep: str = ',',
                         infer_schema: bool = True,
                         ) -> DataFrame:
  
    df = spark.read \
        .format(format)\
        .option('header', 'true')\
        .option('sep', sep)

    if infer_schema: 
        df = df.option('inferSchema', 'true')
    else:
        df = df.option('header', 'false')
    
    df = df.option('encoding', 'latin1')
    df = df.load(path)

    return df

def salvar_tabela_delta_spark(df, path: str, partitionby:list, mode:str="overwrite", merge_schema:bool=False):
    df = df.write \
    .format("delta") \
    .mode(mode) 

    if merge_schema:
        df = df.option("mergeSchema", "true")

    if partitionby:
        df = df.partitionBy(partitionby)

    df.saveAsTable(path)


    print('Tabela salva com sucesso')


def cotacao_atual_etanol_html(url:str, cols_sanitizacao:dict) -> DataFrame:

    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.google.com/'
        }

            
    response = requests.get(url, headers=headers)

    cotacao= pd.read_html(StringIO(response.text), 
                                attrs={'class': 'cot-fisicas'}, 
                                decimal=',', 
                                thousands='.')

    # copia e selecao do valor atual
    cotacao_atual = cotacao[0].copy()

    # sanitização por necessidade do schema
    cotacao_atual.rename(columns=cols_sanitizacao,
                         inplace=True)
    
    cotacao_atual.loc[0,'Data']= cotacao_atual.loc[0,'Data'].split(' - ')[-1].strip()
    
    
    # inserção da coluna com data de ingestao dos dados
    cotacao_atual['Data_Ingestao'] = date.today()

    cotacao_atual['Data'] = pd.to_datetime(cotacao_atual['Data'], format='%d/%m/%Y')
    cotacao_atual['Data_Ingestao'] = pd.to_datetime(cotacao_atual['Data_Ingestao'], format='%Y-%m-%d')

    # transformação para um df spark
    df_spark = spark.createDataFrame(cotacao_atual)

    return df_spark

def cotacao_gasolina_site_petrobras(URL):

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(URL, headers=headers, timeout=10)
    

    soup = BeautifulSoup(response.content, 'html.parser')

    PRECO_ATUAL = soup.find(id='preçomedioBrasil').text

    datas_coleta = soup.find("span", {"data-lfr-editable-id": "telafinal-textoColeta"}).get_text()

    datas_coleta = datas_coleta.split()
    DATA_INICIAL = datas_coleta[0]
    DATA_FINAL = datas_coleta[2]

    return PRECO_ATUAL, DATA_INICIAL, DATA_FINAL


def volume_acucar_exportacao(mes_ano_inicio:str, mes_ano_fim:str):

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    URL = "https://api-comexstat.mdic.gov.br/general?language=pt"
    payload = {
        "flow": "export",
        "monthDetail": True,
        "period": {"from": mes_ano_inicio, "to": mes_ano_fim},
        "filters": [
            {"filter": "ncm", "values": ["17011300"]}
        ],
        "details": ["ncm"],
        "metrics": ["metricKG"]
    }

    response = requests.post(URL, json=payload, verify=False)
    response.raise_for_status()

    data = response.json()["data"]["list"]
    df = pd.DataFrame(data)

    df = df.sort_values(["year", "monthNumber"]).reset_index(drop=True)

    df["date"] = pd.to_datetime(df["year"].astype(str) + "-" +
                                df["monthNumber"].astype(str) + "-01")

    df = df[["date", "year", "monthNumber", "metricKG", "ncm"]]

    return df

    


