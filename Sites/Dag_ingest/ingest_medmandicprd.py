import pendulum
import json
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.operators.python import PythonOperator
#from airflow.providers.google.cloud.hooks.secret_manager import SecretManagerHook
from airflow.models import Variable


db_user = Variable.get("username")
db_password = Variable.get("password")


# =========================================
# Configurações
# =========================================
gcs_bucket = "bucket-slmandic-datalake-hml"
gcs_file_path = "Mandic_DataProc/output/tabelas_para_ingestao_medmandicprd.json"
local_file_path = "/tmp/tabelas_para_ingestao_medmandicprd.json"

# =========================================
# Funções Python
# =========================================
# Lista de tabelas válidas (mantida para validação)
#VALID_TABLES =   ["vw_cursos", "vw_cursos_turmas", "wp_actionscheduler_actions", "wp_actionscheduler_claims", "wp_actionscheduler_groups", "wp_actionscheduler_logs", "wp_commentmeta", "wp_comments", "wp_cs_proof_consent", "wp_cs_scan", "wp_cs_scan_cookies", "wp_cs_scan_scripts", "wp_cs_stats_consent", "wp_cs_unblock_ip", "wp_cwssvgi", "wp_e_events", "wp_e_notes", "wp_e_notes_users_relations", "wp_e_submissions", "wp_e_submissions_actions_log", "wp_e_submissions_values", "wp_fbv", "wp_fbv_attachment_folder", "wp_gf_draft_submissions", "wp_gf_entry", "wp_gf_entry_meta", "wp_gf_entry_notes", "wp_gf_form", "wp_gf_form_meta", "wp_gf_form_revisions", "wp_gf_form_view", "wp_gf_rest_api_keys", "wp_gg_attributes", "wp_gg_galleries", "wp_gg_galleries_resources", "wp_gg_photos", "wp_gg_settings_sets", "wp_gg_tags", "wp_links", "wp_options", "wp_pmxe_exports", "wp_pmxe_google_cats", "wp_pmxe_posts", "wp_pmxe_templates", "wp_pmxi_files", "wp_pmxi_hash", "wp_pmxi_history", "wp_pmxi_images", "wp_pmxi_imports", "wp_pmxi_posts", "wp_pmxi_templates", "wp_postmeta", "wp_posts", "wp_redirection_404", "wp_redirection_groups", "wp_redirection_items", "wp_redirection_logs", "wp_redirects", "wp_revslider_css", "wp_revslider_css_bkp", "wp_revslider_layer_animations", "wp_revslider_layer_animations_bkp", "wp_revslider_navigations", "wp_revslider_navigations_bkp", "wp_revslider_sliders", "wp_revslider_sliders7", "wp_revslider_sliders_bkp", "wp_revslider_slides", "wp_revslider_slides7", "wp_revslider_slides_bkp", "wp_revslider_static_slides", "wp_revslider_static_slides_bkp", "wp_smush_dir_images", "wp_stream", "wp_stream_meta", "wp_term_relationships", "wp_term_taxonomy", "wp_termmeta", "wp_terms", "wp_usermeta", "wp_users", "wp_wc_admin_note_actions", "wp_wc_admin_notes", "wp_wc_category_lookup", "wp_wc_customer_lookup", "wp_wc_download_log", "wp_wc_order_coupon_lookup", "wp_wc_order_product_lookup", "wp_wc_order_stats", "wp_wc_order_tax_lookup", "wp_wc_product_meta_lookup", "wp_wc_reserved_stock", "wp_wc_tax_rate_classes", "wp_wc_webhooks", "wp_wfblockediplog", "wp_wfblocks7", "wp_wfconfig", "wp_wfcrawlers", "wp_wffilechanges", "wp_wffilemods", "wp_wfhits", "wp_wfhoover", "wp_wfissues", "wp_wfknownfilelist", "wp_wflivetraffichuman", "wp_wflocs", "wp_wflogins", "wp_wfls_2fa_secrets", "wp_wfls_role_counts", "wp_wfls_settings", "wp_wfnotifications", "wp_wfpendingissues", "wp_wfreversecache", "wp_wfsecurityevents", "wp_wfsnipcache", "wp_wfstatus", "wp_wftrafficrates", "wp_wfwaffailures", "wp_woocommerce_api_keys", "wp_woocommerce_attribute_taxonomies", "wp_woocommerce_downloadable_product_permissions", "wp_woocommerce_log", "wp_woocommerce_order_itemmeta", "wp_woocommerce_order_items", "wp_woocommerce_payment_tokenmeta", "wp_woocommerce_payment_tokens", "wp_woocommerce_sessions", "wp_woocommerce_shipping_zone_locations", "wp_woocommerce_shipping_zone_methods", "wp_woocommerce_shipping_zones", "wp_woocommerce_tax_rate_locations", "wp_woocommerce_tax_rates", "wp_wpda_csv_uploads", "wp_wpda_csv_uploads_backup_20210705124647", "wp_wpda_csv_uploads_backup_20210819140807", "wp_wpda_csv_uploads_backup_20210819142940", "wp_wpda_csv_uploads_backup_20210819142942", "wp_wpda_csv_uploads_backup_20210819204627", "wp_wpda_csv_uploads_backup_20210820144140", "wp_wpda_csv_uploads_backup_20210908111448", "wp_wpda_logging", "wp_wpda_logging_backup_20210705124647", "wp_wpda_logging_backup_20210819140807", "wp_wpda_logging_backup_20210819142940", "wp_wpda_logging_backup_20210819142942", "wp_wpda_logging_backup_20210819204627", "wp_wpda_logging_backup_20210820144140", "wp_wpda_logging_backup_20210908111448", "wp_wpda_media", "wp_wpda_media_backup_20210705124647", "wp_wpda_media_backup_20210819140807", "wp_wpda_media_backup_20210819142940", "wp_wpda_media_backup_20210819142942", "wp_wpda_media_backup_20210819204627", "wp_wpda_media_backup_20210820144140", "wp_wpda_media_backup_20210908111448", "wp_wpda_menus", "wp_wpda_menus_backup_20210705124647", "wp_wpda_menus_backup_20210819140807", "wp_wpda_menus_backup_20210819142940", "wp_wpda_menus_backup_20210819142942", "wp_wpda_menus_backup_20210819204627", "wp_wpda_menus_backup_20210820144140", "wp_wpda_menus_backup_20210908111448", "wp_wpda_project", "wp_wpda_project_backup_20210705124647", "wp_wpda_project_backup_20210819140807", "wp_wpda_project_backup_20210819142940", "wp_wpda_project_backup_20210819142942", "wp_wpda_project_backup_20210819204627", "wp_wpda_project_backup_20210820144140", "wp_wpda_project_backup_20210908111448", "wp_wpda_project_page", "wp_wpda_project_page_backup_20210705124647", "wp_wpda_project_page_backup_20210819140807", "wp_wpda_project_page_backup_20210819142940", "wp_wpda_project_page_backup_20210819142942", "wp_wpda_project_page_backup_20210819204627", "wp_wpda_project_page_backup_20210820144140", "wp_wpda_project_page_backup_20210908111448", "wp_wpda_project_table", "wp_wpda_project_table_backup_20210705124647", "wp_wpda_project_table_backup_20210819140807", "wp_wpda_project_table_backup_20210819142940", "wp_wpda_project_table_backup_20210819142942", "wp_wpda_project_table_backup_20210819204627", "wp_wpda_project_table_backup_20210820144140", "wp_wpda_project_table_backup_20210908111448", "wp_wpda_publisher", "wp_wpda_publisher_backup_20210705124647", "wp_wpda_publisher_backup_20210819140807", "wp_wpda_publisher_backup_20210819142940", "wp_wpda_publisher_backup_20210819142942", "wp_wpda_publisher_backup_20210819204627", "wp_wpda_publisher_backup_20210820144140", "wp_wpda_publisher_backup_20210908111448", "wp_wpda_table_design", "wp_wpda_table_design_backup_20210705124647", "wp_wpda_table_design_backup_20210819140807", "wp_wpda_table_design_backup_20210819142940", "wp_wpda_table_design_backup_20210819142942", "wp_wpda_table_design_backup_20210819204627", "wp_wpda_table_design_backup_20210820144140", "wp_wpda_table_design_backup_20210908111448", "wp_wpda_table_settings", "wp_wpda_table_settings_backup_20210705124647", "wp_wpda_table_settings_backup_20210819140807", "wp_wpda_table_settings_backup_20210819142940", "wp_wpda_table_settings_backup_20210819142942", "wp_wpda_table_settings_backup_20210819204627", "wp_wpda_table_settings_backup_20210820144140", "wp_wpda_table_settings_backup_20210908111448", "wp_wpfm_backup", "wp_wpgmza", "wp_wpgmza_admin_notices", "wp_wpgmza_circles", "wp_wpgmza_image_overlays", "wp_wpgmza_maps", "wp_wpgmza_nominatim_geocode_cache", "wp_wpgmza_point_labels", "wp_wpgmza_polygon", "wp_wpgmza_polylines", "wp_wpgmza_rectangles", "wp_wpmailsmtp_debug_events", "wp_wpmailsmtp_tasks_meta", "wp_wpr_above_the_fold", "wp_wpr_rocket_cache", "wp_wpr_rucss_used_css", "wp_wsal_metadata", "wp_wsal_occurrences", "wp_wsal_sessions"]
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
                    "main_python_file_uri": "gs://bucket-slmandic-datalake-hml/Mandic_DataProc/Script_DataProc/spark_ingest_medmandicprd.py",
                    "args": [
                        table_name,
                        f"--db_user={db_user}",
                        f"--db_password={db_password}"
                    ],
                    "jar_file_uris": [
                        "gs://bucket-slmandic-datalake-hml/Mandic_DataProc/java/spark-3.1-bigquery-0.42.4.jar"
                    ],
                },
            },
            dag=context["dag"],
        )
        
        # O método execute() é chamado diretamente para garantir a execução sequencial.
        dataproc_job.execute(context=context)


with DAG(
    dag_id="ingest_medmandicprd",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ingestao", "medmandicprd"],
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
