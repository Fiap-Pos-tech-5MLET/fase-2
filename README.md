# Projeto Tech Chalenge Fase-2
Pipeline Batch Bovespa: ingestão e arquitetura de dados Construa um pipeline de dados completo para extrair, processar e analisar dados do pregão da B3, utilizando AWS S3, Glue, Lambda e Athena.

---
## 📌 Índice

- [📝 Sobre o Projeto](#-sobre-o-projeto)
- [🛠 Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [🧱 Arquitetura](#-arquitetura)
- [🎥 Vídeo Demonstrativo](#-vídeo-demonstrativo)
- [🤝 Desenvolvedores](#-desenvolvedores)
---


## 📝 Sobre o Projeto
---

## 🛠 Tecnologias Utilizadas
- **Python 3.13**
- **PySpark**
- **AWS Glue**
- **AWS Lambda**
- **AWS Simple Storage System**
- **AWS Event Bridge**
- **GitHub Actions**
---

## 🧱 Arquitetura

![alt text](docs/imgs/api_fase2.jpg) 
---

### 🗂️ Estrutura de diretorios

| 💎 Nome do Objetivo | 🪄Tipo | 📁 Diretório | ✨ Descrição |
|----------|----------|----------|----------|
| Lambda Extract Bovespa                             | `Lambda`       | src/lambdas/lambda-extract-bovespa/lambda_function.py                                       | Função Lambda responsável por extrair dados da B3 e salvar no S3 em formato Parquet.                              |
| Lambda Trigger Glue Bovespa                        | `Lambda`       | src/lambdas/lambda-trigger-glue-bovespa/lambda_function.py                                  | Função Lambda que dispara a execução de um job Glue ao ser acionada por eventos.                                  |
| EventBridge Start Lambda Extract Bovespa           | `EventBridge`  | src/event-bridge/start-lambda-lambda-extract-bovespa/start-lambda-lambda-extract-bovespa.json| Template CloudFormation para criar uma regra EventBridge que aciona a Lambda de extração da Bovespa.              |
| EventBridge Create Event Raw File Bovespa          | `EventBridge`  | src/event-bridge/create-event-raw-file-bovespa/create-event-raw-file-bovespa.json           | Template CloudFormation para criar uma regra EventBridge que aciona a Lambda ao criar arquivos raw.               |
| Glue Refined Zone Bovespa                          | `Glue`         | src/glue/glue-refined-zone-bovespa/glue-refined-zone-bovespa.*                              | ETL dos dados brutos do Bovespa, disponibilizando dados confiáveis e consistentes na camada refined.

---

## 🎥 Vídeo Demonstrativo
Link para o `video explicativo do projeto` e seu desenvolvimento https://sssssssssssss

---

## 🤝 Desenvolvedores

- RM364306 - Lucas Felipe de Jesus Machado
- RM364480 - Antônio Teixeira Santana Neto
- RM364538 - Gabriela Moreno Rocha dos Santos
- RM364379 - Erik Douglas Alves Gomes
- RM364648 - Leonardo Fernandes Soares