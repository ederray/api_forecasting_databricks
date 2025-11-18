-- SCHEMA ARQUITETURA MEDALLION

-- #### Definição do catálogo de dados ####
USE CATALOG `api-forecasting`;

-- #### 1. Construção da camada Bronze ####
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Dados brutos. Ingestão de dados via webscrapping.';

-- #### 2. Construção da camada Silver ####
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Dados limpos e estruturados pelo processo de Feature engineering.';

-- #### 3. Construção da camada Gold ####
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Features finais para ML.';
