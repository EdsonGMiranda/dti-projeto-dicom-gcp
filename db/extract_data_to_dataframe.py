import pandas as pd
import os
from pathlib import Path

from SQLtoGCP.db.conect_sql import conectar_sql_server
from SQLtoGCP.utils.config import *


def extract_data_to_sql(sql, dbname, env='dev'):
    environment = env
    config = load_env_variables(environment)
    dbname = dbname

    driver = config.get("DRIVER")
    server = config.get(f"SERVER_{dbname}")
    database = config.get(f"DATABASE_{dbname}")
    username = config.get(f"USERNAME_{dbname}")
    password = config.get(f"PASSWORD_{dbname}")

    conexao = conectar_sql_server(driver, server, database, username, password)
    print(conexao)
    query_result = pd.read_sql_query(sql, conexao)

    return query_result


def create_csv(query_result, directory='//opt//airflow//dags//dti-projeto-dicom-gcp//files//'):
    df = query_result
    caminho_arquivo = os.path.join(directory, "vwProcessoSeletivoDadosBrutos.csv")

    df.to_csv(caminho_arquivo, sep=',')



def dataframe_to_parquet(df: pd.DataFrame, table_name: str):
    file_path = Path(r'//opt//airflow//dags//dti-projeto-dicom-gcp//data//')
    
    if df is not None:
        try:
            for column in df.columns:
                if df[column].dtype == 'object':
                    try:
                        df[column] = df[column].astype(str)
                    except Exception as e:
                        print(f"Erro ao converter coluna '{column}' para string: {e}")

                if 'date' in column.lower():
                    try:
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    except Exception as e:
                        print(f"Erro ao converter coluna '{column}' para datetime: {e}")

            df.columns = [col.lower() for col in df.columns]
            print(df.columns)
            caminho_completo = os.path.join(file_path, f'{table_name}.parquet')
            df.to_parquet(caminho_completo)
            print(f"Tabela '{table_name}' salva com sucesso em formato Parquet.")

            return caminho_completo  # Retorna o caminho completo do arquivo salvo

        except Exception as e:
            print(f"Erro ao salvar a tabela '{table_name}' em formato Parquet: {e}")
            return None  # Retorna None em caso de erro
    else:
        print(f"O DataFrame fornecido é None, não foi possível salvar.")
        return None  # Retorna None se o DataFrame for None



