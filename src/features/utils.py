from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
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

def salvar_tabela_delta_spark(df, path: str):
    df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(path)
    print('Tabela salva com sucesso')

