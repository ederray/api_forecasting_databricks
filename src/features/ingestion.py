from pyspark.sql import SparkSession, DataFrame
from datetime import date
from io import StringIO
import requests
import pandas as pd
from bs4 import BeautifulSoup
import urllib3 
from src.features.data import criar_tabela_spark

spark = SparkSession.builder.appName("API_Forecasting_Ingestion").getOrCreate()


def cotacao_dolar_ptax(API_BC_URL:str,
                       DATA_INICIAL:str,
                       DATA_FINAL:str ) -> DataFrame:

    endpoint = (
        "CotacaoDolarPeriodo("
        "dataInicial=@dataInicial,"
        "dataFinalCotacao=@dataFinalCotacao)?"
        f"@dataInicial='{DATA_INICIAL}'&"
        f"@dataFinalCotacao='{DATA_FINAL}'&"
        "$format=json"
    )

    url = API_BC_URL + endpoint

    response = requests.get(url, timeout=60)

    if response.status_code != 200:
        print(response.text)
        raise Exception(f"Erro {response.status_code} na API do BC")

    data = response.json()["value"]
    df = pd.DataFrame(data)
    
    return df


def cotacao_atual_etanol_html(url:str) -> DataFrame:

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
    
    cotacao_atual.loc[0,'Data']= cotacao_atual.loc[0,'Data'].split(' - ')[-1].strip()

    cotacao_atual['Data'] = pd.to_datetime(cotacao_atual['Data'], format='%d/%m/%Y')

    return cotacao_atual

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


def volume_acucar_exportacao(mes_ano_inicio:str, 
                             mes_ano_fim:str) ->DataFrame:

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

    


