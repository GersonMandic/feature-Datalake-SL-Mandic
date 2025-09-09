import os
import sys
import json
import tempfile
from pyspark.sql import SparkSession
from google.cloud import storage, bigquery
import argparse
#from google.cloud import secretmanager

# =========================================
# Configurações
# =========================================
project_id = "slmandic-datalake-hml"
secret_name = "mandic-slmglpi"
jdbc_url = "jdbc:mysql://slm-sites.c0appmybhqbr.us-east-1.rds.amazonaws.com:3306/slmandiclimeiraprd"
bucket_name = "bucket-slmandic-datalake-hml"
blob_name = "Mandic_DataProc/credenciais/slmandic-datalake-hml-fe6aee5affe8.json"
bq_table = "slmandic-datalake-hml.bq_dataset_slmandic_datalake_hml.Historico_Execucao"
output_blob_name = "Mandic_DataProc/output/tabelas_para_ingestao_slmandiclimeiraprd.json"
parser = argparse.ArgumentParser()
parser.add_argument("--db_user", required=True, help="Usuário do banco de dados.")
parser.add_argument("--db_password", required=True, help="Senha do banco de dados.")

# =========================================
# Autenticação GCP
# =========================================
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)
temp_cred_file = tempfile.NamedTemporaryFile(delete=False)
blob.download_to_filename(temp_cred_file.name)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_cred_file.name

args = parser.parse_args()

db_user = args.db_user
db_password = args.db_password

# =========================================
# Spark Session
# =========================================
spark = SparkSession.builder.appName("VerificaCarga").getOrCreate()


# =========================================
# Busca update_time no Aurora
# =========================================
def get_last_update_sys():
    #creds = get_db_credentials_from_secret_manager()
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    query = f"""
        (SELECT table_name, update_time 
         FROM information_schema.tables 
         WHERE table_schema = 'slmandiclimeiraprd') as t
    """

    df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
    return {row["table_name"]: row["update_time"] for row in df.collect()}

# =========================================
# Busca última execução no BigQuery
# =========================================
def get_last_execution_bq():
    client = bigquery.Client()
    
    # Adicionando um filtro de partição à sua consulta
    # O exemplo abaixo filtra os dados dos últimos 30 dias.
    # Ajuste o valor conforme a sua necessidade de negócio.
    query = f"""
        SELECT table_name, MAX(execution_time) as last_exec
        FROM `{bq_table}`
        WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        GROUP BY table_name
    """
    
    results = client.query(query).result()
    return {row.table_name: row.last_exec for row in results}

# =========================================
# Lógica principal
# =========================================
def main():
    sys_updates = get_last_update_sys()
    bq_execs = get_last_execution_bq()

    tables_to_update = []
    for table, sys_time in sys_updates.items():
        bq_time = bq_execs.get(table)

        if bq_time is None:
            tables_to_update.append(table)
        elif sys_time is None:
            continue
        elif sys_time > bq_time:
            tables_to_update.append(table)
    
    # Salva a lista de tabelas em um arquivo JSON
    tables_json = json.dumps(tables_to_update)
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_file.write(tables_json)
        temp_file_path = temp_file.name

    # Faz o upload do arquivo para o GCS
    output_blob = bucket.blob(output_blob_name)
    output_blob.upload_from_filename(temp_file_path)
    print(f"Lista de tabelas salva em gs://{bucket_name}/{output_blob_name}")
    
    # Limpeza do arquivo temporário
    os.remove(temp_file_path)

if __name__ == "__main__":
    main()
    spark.stop()