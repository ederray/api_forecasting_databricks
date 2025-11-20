-- CONSTRUÇÃO DO VOLUME HISTÓRICO DE DADOS
CREATE VOLUME IF NOT EXISTS bronze.arquivos_brutos
COMMENT 'Volume para arquivos brutos da Camada Bronze.'
%load_ext autoreload
%autoreload 2

 
The autoreload extension is already loaded. To reload it, use:
  %reload_ext autoreload
# cotacao do preço do Etanol Hidratado Comum (EHC)

# preço do açucar

# producao de safra de cana de acucar

# cambio real - dolar

# preço do petróleo/gasolina

# media de tempeatura nas regioes produtoras

# media de volume de chuva nas regioes produtoras

# volume de exportação


 
# carregamento de bibliotecas
import sys
import os

root = os.path.abspath(os.path.join(os.getcwd(), "../../"))

if root not in sys.path:
    sys.path.append(root)


from src.features.utils import *



 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Ingestion").getOrCreate()
 
# construção da tabela histórica de preços do etanol hidratado industrial
PATH_ARQUIVO_HISTORICO = "/Volumes/api-forecasting/bronze/arquivos_brutos/cepea-consulta-20251119.csv"
TABELA_HISTORICA = "api-forecasting.bronze.precos_cepea_historico_raw"

df =  carregar_tabela_spark(format='csv', 
                            path=PATH_ARQUIVO_HISTORICO, 
                            sep=';', infer_schema=True)
df.show()


df:pyspark.sql.connect.dataframe.DataFrame = [ï»¿Data: date, Ã vista R\$: string ... 1 more field]
+----------+-----------+------------+
|   ï»¿Data|Ã vista R\$|Ã vista US\$|
+----------+-----------+------------+
|2015-01-02|     1,3221|      0,4926|
|2015-01-09|     1,2899|      0,4803|
|2015-01-16|     1,3461|      0,5108|
|2015-01-23|     1,3669|      0,5245|
|2015-01-30|     1,4117|      0,5419|
|2015-02-06|     1,4511|      0,5308|
|2015-02-13|     1,4619|      0,5167|
|2015-02-20|     1,4381|      0,5028|
|2015-02-27|     1,3444|      0,4697|
|2015-03-06|     1,3132|      0,4422|
|2015-03-13|     1,3242|      0,4198|
|2015-03-20|     1,2705|      0,3912|
|2015-03-27|     1,3137|      0,4132|
|2015-04-10|     1,3064|      0,4226|
|2015-04-17|     1,2965|      0,4244|
|2015-04-24|     1,2818|      0,4283|
|2015-04-30|     1,3019|      0,4405|
|2015-05-08|     1,2568|      0,4131|
|2015-05-15|     1,2609|      0,4174|
|2015-05-22|     1,2620|      0,4154|
+----------+-----------+------------+
only showing top 20 rows
Last execution failed
6
6
12
# persistência da tabela na camada bronze
salvar_tabela_delta_spark(df, TABELA_HISTORICA)
1
[INVALID_IDENTIFIER] The unquoted identifier api-forecasting is invalid and must be back quoted as: `api-forecasting`.
Unquoted identifiers can only contain ASCII letters ('a' - 'z', 'A' - 'Z'), digits ('0' - '9'), and underbar ('_').
Unquoted identifiers must also not start with a digit.
Different data sources and meta stores may impose additional restrictions on valid identifiers. SQLSTATE: 42602
05:07 PM (2s)
7
7
%sql

-- O catálogo deve ser 'api-forecasting'
SELECT current_catalog();

-- O schema deve ser 'default' ou 'bronze' (idealmente bronze)
SELECT current_schema();

-- Tenta listar os schemas (deve retornar bronze)
SHOW SCHEMAS IN `api-forecasting`;
_sqldf:pyspark.sql.connect.dataframe.DataFrame = [databaseName: string]

Table
5 rows
|
2.02s runtime
Refreshed 6 minutes ago
This result is stored as _sqldf and can be used in other Python and SQL cells.
[Shift+Enter] to run and move to next cell
[Ctrl+Shift+P] to open the command palette
[Esc H] to see all keyboard shortcuts
02_create_volume.sql
$0