from pyspark.sql import SparkSession, DataFrame
from datetime import date
from io import StringIO
import requests
import pandas as pd
from bs4 import BeautifulSoup
import urllib3 


spark = SparkSession.builder.appName("API_Forecasting_Processing").getOrCreate()


def renomear_colunas(dicionario:dict, 
                     df:DataFrame, df_spark:bool=False) -> DataFrame:
  
  if not df_spark:
    df.rename(columns=dicionario, inplace=True)

  else:
    for chave, valor in dicionario.items():
      df = df.withColumnRenamed(chave, valor)
  return df


def data_ingestao(df:DataFrame) -> DataFrame:
  
  df['Data_Ingestao'] = date.today()

  return df