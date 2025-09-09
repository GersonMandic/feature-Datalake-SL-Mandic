import sys
import time
from pyspark.sql import SparkSession
import os
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType
from pyspark.sql.functions import current_timestamp, to_date
from datetime import datetime, date
import json
import argparse

from google.cloud import storage
import tempfile

# Cria um parser de argumentos
parser = argparse.ArgumentParser()
parser.add_argument("table_name", help="O nome da tabela a ser ingerida.")
parser.add_argument("--db_user", required=True, help="Usuário do banco de dados.")
parser.add_argument("--db_password", required=True, help="Senha do banco de dados.")

# ID do seu projeto GCP e o nome do secret
project_id = "slmandic-datalake-hml"
data_base = "mandicdigitalprd_raw"
secret_name = "mandic-slmglpi"

# Configurações do banco de dados (o restante é fixo)
#jdbc_url = "jdbc:mysql://slm-sites.c0appmybhqbr.us-east-1.rds.amazonaws.com:3306/mandicdigitalprd"
jdbc_url = "jdbc:mysql://slm-sites.c0appmybhqbr.us-east-1.rds.amazonaws.com:3306/mandicdigitalprd?zeroDateTimeBehavior=CONVERT_TO_NULL"

bucket_name = "bucket-slmandic-datalake-hml"
blob_name = "Mandic_DataProc/credenciais/slmandic-datalake-hml-fe6aee5affe8.json"  # ajuste o nome do arquivo conforme necessário

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)
temp_cred_file = tempfile.NamedTemporaryFile(delete=False)
blob.download_to_filename(temp_cred_file.name)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_cred_file.name

# table_name = sys.argv[1]

# Parse os argumentos
args = parser.parse_args()

table_name = args.table_name
db_user = args.db_user
db_password = args.db_password

spark = SparkSession.builder \
    .appName(f"Ingest_{table_name}") \
    .getOrCreate()

start_time = time.time()


properties = {
    "user": db_user,
    "password": db_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Lê dados do MySQL usando as credenciais seguras
df = spark.read.jdbc(jdbc_url, table_name, properties=properties)

schema = df.schema

# Função para converter tipos do Spark para BigQuery
def spark_to_bq_type(spark_type):
    mapping = {
        "StringType": "STRING",
        "IntegerType": "INT64",
        "LongType": "INT64",
        "DoubleType": "FLOAT64",
        "FloatType": "FLOAT64",
        "BooleanType": "BOOL",
        "TimestampType": "TIMESTAMP",
        "DateType": "DATE",
        "ShortType": "INT64",
        "BinaryType": "BYTES"
    }
    return mapping.get(spark_type, "STRING")

# Gera o DDL para BigQuery
ddl_fields = []
for field in schema.fields:
    bq_type = spark_to_bq_type(type(field.dataType).__name__)
    ddl_fields.append(f"{field.name} {bq_type}")

ddl = f"CREATE TABLE IF NOT EXISTS `slmandic-datalake-hml.{data_base}.{table_name}` ({', '.join(ddl_fields)})"

# Cria a tabela no BigQuery se não existir
from google.cloud import bigquery
bq_client = bigquery.Client()
try:
    bq_client.query(ddl).result()
except Exception as e:
    print(f"Erro ao criar tabela: {e}")

df.write.format("com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider") \
    .option("table", f"{data_base}.{table_name}") \
    .option("temporaryGcsBucket", "bucket-slmandic-datalake-hml") \
    .mode("overwrite") \
    .save()

# ============================================
# Calcula os metadados
# ============================================
row_count = df.count()
execution_time_sec = time.time() - start_time
execution_date = datetime.now().date()
execution_time = datetime.now()


# Criando DataFrame com schema explícito
schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("name_dataBase", StringType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("execution_date", DateType(), True),
    StructField("execution_time", TimestampType(), True),
])

metadata_df = spark.createDataFrame([
    Row(
        table_name=table_name,           # String
        name_dataBase=data_base,         # String
        row_count=row_count,                # int
        execution_date=execution_date,   # Date
        execution_time=execution_time  # Timestamp

    )
], schema=schema)


# Grava no BigQuery (modo append)
bq_table = "Historico_Execucao"
metadata_df.write.format("com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider") \
    .option("table", f"bq_dataset_slmandic_datalake_hml.{bq_table}") \
    .option("temporaryGcsBucket", "bucket-slmandic-datalake-hml") \
    .mode("append") \
    .save()

print(f"Metadados gravados/atualizados no BigQuery: row_count={row_count}, execution_time={execution_time_sec:.2f}s")
