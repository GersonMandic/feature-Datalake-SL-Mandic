import pendulum
import json
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


db_user = Variable.get("usernameServer")
db_password = Variable.get("passwordServer")


# =========================================
# Configurações
# =========================================
gcs_bucket = "bucket-slmandic-datalake-hml"
gcs_file_path = "Mandic_DataProc/output/tabelas_para_ingestao_TOTVSAUDIT.json"
local_file_path = "/tmp/tabelas_para_ingestao_TOTVSAUDIT.json"

# =========================================
# Funções Python
# =========================================
def read_tables_from_file():
    """Lê a lista de tabelas do arquivo local e retorna como uma lista."""
    try:
        with open(local_file_path, "r") as f:
            tables = json.load(f)
            return tables
    except FileNotFoundError:
        print(f"O arquivo {local_file_path} não foi encontrado.")
        return []
    except json.JSONDecodeError:
        print(f"Erro ao decodificar o JSON do arquivo {local_file_path}.")
        return []

def process_and_run_tables(**context):
    """
    Lê a lista de tabelas do XCom, obtém as credenciais do Secret Manager
    e dispara os jobs Dataproc sequencialmente.
    """
    ti = context["ti"]
    tables = ti.xcom_pull(task_ids="read_tables_task")

    if not tables:
        print("A lista de tabelas está vazia. Nenhuma tabela será processada.")
        return

    if not db_user or not db_password:
        raise ValueError("Não foi possível obter as credenciais do Secret Manager.")

    # Executa o job Dataproc para cada tabela sequencialmente
    for table_name in tables:
       
        dataproc_job = DataprocSubmitJobOperator(
            task_id=f"spark_ingest_{table_name}",
            project_id="slmandic-datalake-hml",
            region="us-central1",
            job={
                "reference": {"project_id": "slmandic-datalake-hml"},
                "placement": {"cluster_name": "dataproc-cluster-slmandic-datalake-hml"},
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket-slmandic-datalake-hml/Mandic_DataProc/Script_DataProc/spark_ingest_TOTVSAUDIT.py",
                    "args": [
                        table_name,
                        f"--db_user={db_user}",
                        f"--db_password={db_password}"
                    ],
                    "jar_file_uris": [
                        "gs://bucket-slmandic-datalake-hml/Mandic_DataProc/java/mssql-jdbc-13.2.0.jre8.jar"
                    ],
                },
            },
            dag=context["dag"],
        )
        
        # O método execute() é chamado diretamente para garantir a execução sequencial.
        dataproc_job.execute(context=context)


with DAG(
    dag_id="ingest_TOTVSAUDIT",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ingestao", "TOTVSAUDIT"],
) as dag:

    # 1. Baixa o arquivo do GCS para o ambiente do Airflow
    download_tables_list = GCSToLocalFilesystemOperator(
        task_id="download_tables_list",
        bucket=gcs_bucket,
        object_name=gcs_file_path,
        filename=local_file_path,
    )

    # 2. Lê a lista de tabelas do arquivo baixado
    read_tables_task = PythonOperator(
        task_id="read_tables_task",
        python_callable=read_tables_from_file,
    )

    # 3. Processa e dispara os jobs sequencialmente
    sequential_ingestion = PythonOperator(
        task_id="sequential_ingestion",
        python_callable=process_and_run_tables,
    )

    # Define a sequência de execução
    download_tables_list >> read_tables_task >> sequential_ingestion