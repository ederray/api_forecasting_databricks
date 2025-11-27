# Forecasting do Preço do Etanol Hidratado Industrial  
Plataforma de Data & MLOps para time de Sourcing e Supply Chain

---

## 1. Objetivo do Projeto

O objetivo deste projeto é desenvolver uma Plataforma de Data & MLOps capaz de realizar o forecasting e o monitoramento contínuo do preço do Etanol Hidratado Industrial no Brasil, apoiando o time de Sourcing na tomada de decisões de compra desse insumo crítico para a fabricação de produtos derivados.

A solução entrega visibilidade antecipada de custos para decisões estratégicas de compras e suprimentos, permitindo:

- Otimizar o timing de compra  
- Reduzir a volatilidade do orçamento  
- Apoiar operações de hedge  
- Melhorar o planejamento de produção e logística  
- Reduzir o impacto do custo de aquisição de insumo no CPV.

---

## 2. Contexto de Negócio: Importância do Etanol

A escolha do Etanol Hidratado Industrial/Químico como commodity é estratégica devido à sua relevância para a matriz energética e industrial do Brasil.

### Matriz Energética  
O etanol é um pilar da matriz de transportes brasileira. A paridade de preço Etanol/Gasolina influencia consumo, competitividade e decisões de destinação da cana (açúcar vs etanol).

### Relevância Econômica  
A indústria sucroenergética é crítica para o PIB e para a balança comercial. Seu preço é sensível a fatores como safra, clima, preço do petróleo e logística.

### Etanol Industrial/Químico  
O foco no etanol utilizado como insumo industrial atende setores como químico, farmacêutico e de bebidas, onde ele possui alto impacto no custo e exige previsões mais precisas.

---

## 3. Posicionamento Estratégico (Matriz de Kraljic)

A Matriz de Kraljic classifica itens conforme Impacto no Lucro (eixo Y) e Risco de Fornecimento (eixo X).

### Posicionamento do Etanol Industrial/Químico

| Categoria      | Risco de Fornecimento | Impacto no Lucro | Análise |
|----------------|------------------------|------------------|---------|
| Estratégico    | Alto                   | Alto             | Item crítico. Requer parcerias e forecasting preciso. |

Justificativa: o preço do etanol é volátil (safra, petróleo, clima). Em indústrias químicas e farmacêuticas, ele é um insumo essencial de alto impacto no custo, não tendo substituição direta e exigindo políticas de strategic sourcing bem definidas.

---

## 4. Arquitetura do Projeto (Data Lakehouse & MLOps)

O projeto utiliza uma arquitetura Data Lakehouse em Databricks para governança, escalabilidade e ACID compliance.

### 4.1 Tecnologias Principais

- Databricks (Workspace & Repos)  
- PySpark  
- Delta Lake  
- Python (requests, BeautifulSoup, APIs)  
- Databricks Jobs/Workflows  
- MLflow (tracking, registro, monitoramento)

### 4.2 Arquitetura Medallion

| Camada | Nome    | Responsabilidade |
|--------|---------|------------------|
| Bronze | raw     | Dados brutos ingestados. Histórico e incremental. |
| Silver | trusted | Feature engineering, imputação, criação de lags, criação do target. |
| Gold   | curated | Features finais, modelo serializado e previsões. |

## 5. Estrutura de Diretórios
```bash
api-forecasting/
├── notebooks/ # Orquestração (Bronze -> Silver -> Gold)
│ ├── bronze/
│ ├── silver/
│ └── gold/
├── sql/ # Queries SQL auxiliares
└── src/ # Lógica de negócio
└── features/
├── init.py
├── data_utils.py # I/O e persistência Delta Lake
├── ingestion_utils.py # Ingestão via APIs / scraping
└── processing.py # Transformações (lags, spread, imputação)

```
## 6. Status Atual do Projeto (Camada Silver)

- **Bronze:** Finalizada e validada  
- **Silver:** Em desenvolvimento  
  - Módulo em foco: `src/features/processing.py`  
  - Notebook atual: `03_feature_engineering_silver.ipynb`  

### Próximos Passos
- Cálculo do spread  
- Geração de lags e rolling windows  
- Prevenção de data leakage  
- Preparação para camada Gold  

---

## 7. Resumo da Solução

Solução completa de previsão semanal do preço do etanol industrial com horizonte de 5 a 15 semanas para apoiar estratégias de hedge, compras e negociação.

Componentes:

- Ingestão automatizada  
- Arquitetura Medallion com Delta Live Tables (DLT)  
- Modelo preditivo monitorado via MLflow  
- API para consumo das previsões  
- Dashboard analítico  
- Camada LLM/RAG para monitoramento de riscos e rupturas  

---


