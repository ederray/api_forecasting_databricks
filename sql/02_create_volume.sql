-- CONSTRUÇÃO DO VOLUME HISTÓRICO DE DADOS
CREATE VOLUME IF NOT EXISTS bronze.arquivos_brutos
COMMENT 'Volume para arquivos brutos da Camada Bronze.'
