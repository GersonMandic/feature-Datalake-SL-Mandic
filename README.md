Pipeline de Ingestão de Dados - SLMandic (MySQL para BigQuery)
Visão Geral de Negócios
Este documento descreve o processo automatizado de carga de dados da base *DataBase* para o nosso Data Lake. O objetivo principal é garantir que os dados estejam sempre atualizados e disponíveis para análises estratégicas, permitindo uma fonte única de verdade e tomada de decisões ágeis.

O processo opera de forma programada, verificando se houve alguma alteração nas tabelas de origem e, em caso afirmativo, disparando a ingestão para o BigQuery. Isso otimiza o uso de recursos, pois somente os dados que foram alterados são processados.

Arquitetura da Solução
A solução é construída com base na infraestrutura do Google Cloud Platform (GCP) e orquestrada pelo Apache Airflow.

Componentes Principais:

MySQL Database (AWS Aurora): Fonte dos dados originais (*DataBase*).

Apache Airflow: Orquestra a execução de duas DAGs que controlam todo o fluxo de ingestão.

Dataproc: Cluster Spark gerenciado que processa e move os dados do MySQL para o BigQuery.

Google BigQuery: Destino final dos dados, atuando como o principal repositório no Data Lake.

Google Cloud Storage (GCS): Armazena os scripts do Spark, dependências e arquivos intermediários.

!



Documentação Técnica
1. Estrutura do Projeto
O pipeline é composto por duas DAGs principais do Airflow, que trabalham em conjunto:

controller_*DataBase*.py: A DAG principal que verifica quais tabelas precisam ser atualizadas.

ingest_*DataBase*.py: A DAG de ingestão que carrega os dados das tabelas identificadas.

2. Fluxo de Execução
O processo é iniciado diariamente pela DAG controller_*DataBase* e segue os seguintes passos:

Verificação de Carga (verifica_carga_*DataBase*.py):

O script verifica_carga_*DataBase*.py é executado em um job Dataproc.

Ele consulta o banco de dados MySQL para obter as datas da última atualização de cada tabela.

Compara essas datas com o histórico de execuções gravado no BigQuery.

Gera uma lista de tabelas que foram atualizadas e a salva em um arquivo JSON no GCS.

Disparo da Ingestão:

A DAG controller_*DataBase* dispara a DAG ingest_*DataBase*.

Processamento Sequencial (ingest_*DataBase*.py):

Esta DAG baixa a lista de tabelas do GCS.

Para cada tabela na lista, ela dispara um job Dataproc separado.

O script spark_ingest_*DataBase*.py lê os dados da tabela correspondente no MySQL e os grava no BigQuery.

Após a ingestão, os metadados da execução (contagem de linhas, tempo) são registrados na tabela Historico_Execucao no BigQuery.

3. Configurações e Dependências
As configurações e dependências do projeto são gerenciadas no Airflow e no GCP.

Parâmetros:

Projeto GCP: slmandic-datalake-hml

Região do Dataproc: us-central1

Cluster Dataproc: dataproc-cluster-slmandic-datalake-hml

Bucket GCS: bucket-slmandic-datalake-hml

Credenciais:

As credenciais do banco de dados (db_user e db_password) são armazenadas com segurança como variáveis no Airflow (ou Secret Manager) e passadas como argumentos para os jobs Dataproc.

Manutenção e Operação
Monitoramento: Use a interface do Airflow para monitorar o status e o histórico de execuções das DAGs.

Problemas com Credenciais: Caso as credenciais do banco de dados mudem, elas devem ser atualizadas nas variáveis do Airflow.

Escalabilidade: Para grandes volumes de dados, o processo sequencial de ingestão pode ser alterado para paralelizar a carga de tabelas, o que exigiria mudanças na DAG ingest_*DataBase*.