from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.time_delta import TimeDeltaSensor

# Configurações do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='dicom_pipeline_bigquery',
    default_args=default_args,
    schedule_interval=None,  # Ajuste o agendamento conforme necessário
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dicom', 'gcp'],
) as dag:
    
    wait_10_minutes = TimeDeltaSensor(
        task_id='wait_10_minutes',
        delta=timedelta(minutes=10),  
    )
        

    # Task 1: Realizar transformação SQL no dataset de origem e gravar no dataset destino
    transform_table = BigQueryInsertJobOperator(
        task_id='transform_table',
        configuration={
            "query": {
                "query": """
                    SELECT SHA256(nmemail) as nmemail ,cdescola,nmescola,nmprocessoseletivo,CASE WHEN dtinscricao = 'None' THEN NULL ELSE cast (dtinscricao as date) END AS dtinscricao,cast(dtcadastro as date) dtcadastro, ano_ingresso as ano_matricula, cast (dt_matricula as date) as dt_matricula
                    FROM `dicom-gim-eng-dti.dti_bronze_1.ProcessoSeletivoBrutos`
                """,
                "destinationTable": {
                    "projectId": "dicom-gim-eng-dti",
                    "datasetId": "dti_silver_1",
                    "tableId": "ProcessoSeletivoSilver"
                },
                "writeDisposition": "WRITE_TRUNCATE",  # Substituir ou criar a tabela destino
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',  # Conexão configurada no Airflow
    )
    
    # Task 2: Mover a tabela transformada para outro projeto/dataset
    export_table = BigQueryInsertJobOperator(
        task_id='export_table',
        configuration={
            "query": {
                "query": """
                    SELECT *
                    FROM `dicom-gim-eng-dti.dti_silver_1.ProcessoSeletivoSilver`
                """,
                "destinationTable": {
                    "projectId": "dicom-gim",
                    "datasetId": "dti_silver_1",
                    "tableId": "ProcessoSeletivoSilver"
                },
                "writeDisposition": "WRITE_TRUNCATE",  # Substituir ou criar a tabela destino
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default',  # Conexão configurada no Airflow
    )
    
 
    # Ordem das tarefas
    wait_10_minutes >> transform_table >> export_table
