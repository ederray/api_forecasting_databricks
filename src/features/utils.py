from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import date
from io import StringIO
import requests
import pandas as pd

spark = SparkSession.builder.appName("API_Forecasting_Utils").getOrCreate()

def import_query(path):

    with open(path) as file:
        return file.read()
    
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


def cotacao_atual_html(url:str, cols_sanitizacao:dict) -> DataFrame:

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


