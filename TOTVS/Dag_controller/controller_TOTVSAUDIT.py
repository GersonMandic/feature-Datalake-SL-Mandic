import pendulum
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

# Recupera as credenciais do Secret Manager
db_user = Variable.get("usernameServer")
db_password = Variable.get("passwordServer")

# Define o DAG
with DAG(
    dag_id="controller_TOTVSAUDIT",
    schedule_interval="0 12 * * *",
    #schedule_interval='*/15 * * * *',  # roda a cada 15 minutos
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["controller", "TOTVSAUDIT"],
) as dag:

    # 1. Executa o job Dataproc que salva a lista de tabelas no GCS
    verifica_carga = DataprocSubmitJobOperator(
        task_id="verifica_carga",
        project_id="slmandic-datalake-hml",
        region="us-central1",
        job={
            "reference": {"project_id": "slmandic-datalake-hml"},
            "placement": {"cluster_name": "dataproc-cluster-slmandic-datalake-hml"},
            "pyspark_job": {
                "main_python_file_uri": "gs://bucket-slmandic-datalake-hml/Mandic_DataProc/Script_DataProc/verifica_carga_TOTVSAUDIT.py",
                 "args": [
                    f"--db_user={db_user}",
                    f"--db_password={db_password}"
                    ],
                "jar_file_uris": [
                    "gs://bucket-slmandic-datalake-hml/Mandic_DataProc/java/mssql-jdbc-13.2.0.jre8.jar"
                ],
                "python_file_uris": [
                    "gs://bucket-slmandic-datalake-hml/Mandic_DataProc/libs/google-cloud-secret-manager-2.20.2-py2.py3-none-any.whl"
                ],
            },
        },
    )

    # 2. Dispara a DAG de ingestão
    trigger_ingest_dag = TriggerDagRunOperator(
        task_id="trigger_ingest_dag",
        trigger_dag_id="ingest_TOTVSAUDIT",
    )
    
    # Define a sequência de execução
    verifica_carga >> trigger_ingest_dag