from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os

dag_folder = os.path.dirname(os.path.abspath(__file__))
custom_module_path = os.path.join(dag_folder, 'dti-projeto-dicom-gcp', 'utils', 'db', 'static_sources', 'sql')
sys.path.append(custom_module_path)

from SQLtoGCP.utils.read_sql_file import read_sql_file
from SQLtoGCP.utils.data_transformations import clean_filename
from SQLtoGCP.db.extract_data_to_dataframe import extract_data_to_sql, dataframe_to_parquet
from SQLtoGCP.db.process_data import delete_flex_field_cols, mascarar_coluna_obs, upper_columns_values

@dag(schedule_interval='0 06 * * *', start_date=datetime(2025, 1, 28), catchup=True, tags=["dicom", "gcp"])
def pipeline_dicom_bi_cgp1():
    start_process = DummyOperator(task_id="start_process")
    end_process = DummyOperator(task_id="end_process")

    @task
    def extract_data_processoseletivo():
        filename = 'vwProcessoSeletivoDadosBrutos.sql'
        sql = read_sql_file(filename)
        return sql

    @task
    def transform_data_processoseletivo(sql):      
        raw_data = extract_data_to_sql(sql, dbname='BI', env='prd')
        parquet_name = 'vwProcessoSeletivoDadosBrutos'
        end_parquet = dataframe_to_parquet(raw_data, parquet_name)
        print(f'nome do parquet: {end_parquet}')
        return end_parquet

    @task
    def load_data_processoseletivo(end_parquet):
        data = datetime.now()
        file_name = clean_filename(end_parquet)
        caminho_arquivo = os.path.join("/opt/airflow/dags/dti-projeto-dicom-gcp/data/", file_name)
        output_file = clean_filename(caminho_arquivo)

        BUCKET_NAME = "dti-dicom-pipeline-cgp"
        LOCAL_FILE_PATH = f"{caminho_arquivo}"
        GCS_OBJECT_NAME = f"{output_file}"

        upload_operator = LocalFilesystemToGCSOperator(
            task_id="upload_file_to_gcs",
            src=LOCAL_FILE_PATH,
            dst=GCS_OBJECT_NAME,
            bucket=BUCKET_NAME,
            gcp_conn_id="google_cloud_default",
        )
        upload_operator.execute({})

    @task
    def extract_data_lyceum_corp():
        tables = ['LYALUNO']
        databases = ['LYCEUMSP']
        sql_statements = []
        for database in databases:
            for table in tables:
                sql = read_sql_file(f'{table}.sql')
                sql_statements.append(sql)
        return sql_statements

    @task
    def transform_data_lyceum_corp(sql_statements):
        processed_dfs = []
        for sql in sql_statements:
            df = extract_data_to_sql(sql, dbname='LYCEUMSP', env='prd')
            df = delete_flex_field_cols(df)
            df = mascarar_coluna_obs(df, 'OBS_ALUNO_FINAN')
            df = upper_columns_values(df, 'PAIS_2G')
            dir_parquet = dataframe_to_parquet(df, 'LYALUNOLYCEUMSP')
            processed_dfs.append(dir_parquet)
        return processed_dfs

    @task
    def load_data_lyceum_corp(processed_dfs):
        data = datetime.now()
        for end_parquet in processed_dfs:
            file_name = clean_filename(end_parquet)
            caminho_arquivo = os.path.join("/opt/airflow/dags/dti-projeto-dicom-gcp/data/", file_name)
            output_file = clean_filename(caminho_arquivo)

            BUCKET_NAME = "dti-dicom-pipeline-cgp"
            LOCAL_FILE_PATH = f"{caminho_arquivo}"
            GCS_OBJECT_NAME = f"{output_file}"

            upload_operator = LocalFilesystemToGCSOperator(
                task_id=f"upload_file_{file_name}",
                src=LOCAL_FILE_PATH,
                dst=GCS_OBJECT_NAME,
                bucket=BUCKET_NAME,
                gcp_conn_id="google_cloud_default",
            )
            upload_operator.execute({})

    # Operador que inicia o pipeline no Data Fusion
    start_pipeline_datafusion = CloudDataFusionStartPipelineOperator(
        task_id='start_datafusion_pipeline',
        location='southamerica-east1',
        instance_name='dti-pipeline-dicom',
        pipeline_name='dti-bucket-to-bigquery',
        runtime_args={},
        project_id='dicom-gim-eng-dti',
        gcp_conn_id='google_cloud_default',
        asynchronous=True,  # Altere para False para esperar o pipeline terminar
    )
      

    task_dicom_pipeline_bigquery = TriggerDagRunOperator(
        task_id='task_dicom_pipeline_bigquery',
        trigger_dag_id='dicom_pipeline_bigquery',  # Nome da DAG que será acionada
        conf={"message": "Triggered by parent_dag"},
        execution_timeout=timedelta(seconds=600),# Passa parâmetros para a DAG filha
    )
    
    processoseletivo_extracted = extract_data_processoseletivo()
    processoseletivo_transformed = transform_data_processoseletivo(processoseletivo_extracted)
    processoseletivo_loaded = load_data_processoseletivo(processoseletivo_transformed)

    lyceum_corp_extracted = extract_data_lyceum_corp()
    lyceum_corp_transformed = transform_data_lyceum_corp(lyceum_corp_extracted)
    lyceum_corp_loaded = load_data_lyceum_corp(lyceum_corp_transformed)

    # Dependências
    start_process >> [processoseletivo_extracted, lyceum_corp_extracted]
    processoseletivo_extracted >> processoseletivo_transformed
    lyceum_corp_extracted >> lyceum_corp_transformed 
    [processoseletivo_loaded, lyceum_corp_loaded] >> start_pipeline_datafusion >> task_dicom_pipeline_bigquery >> end_process

pipeline_dicom_bi_cgp1()
