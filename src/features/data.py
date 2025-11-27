from pyspark.sql import SparkSession, DataFrame
import pandas as pd


spark = SparkSession.builder.appName("API_Forecasting_Data").getOrCreate()

def import_query(path):

    with open(path) as file:
        return file.read()


def criar_tabela_spark(df: DataFrame, colunas:list=None) ->DataFrame:
    if not colunas:
        df = spark.createDataFrame(df)
    else:
        df = spark.createDataFrame(df, colunas)
    
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


def salvar_tabela_delta_spark(df, 
                              path: str, 
                              partitionby:list, 
                              mode:str="overwrite", 
                              merge_schema:bool=False):
    
    df = df.write \
    .format("delta") \
    .mode(mode) 

    if merge_schema:
        df = df.option("mergeSchema", "true")

    if partitionby:
        df = df.partitionBy(partitionby)

    df.saveAsTable(path)


    print('Tabela salva com sucesso')






    


