
from utils.read_sql_file import read_sql_file
from db.extract_data_to_dataframe import *
from db.process_data import *

sql = read_sql_file('vwProcessoSeletivoDadosBrutos.sql','sql/')

df = extract_data_to_sql(sql,dbname='BI', env='prd')

dataframe_to_parquet(df, f'vwProcessoSeletivoDadosBrutos')


tables = ['LY_ALUNO']

databases = ['LYCEUM_SP']

for database in databases:
    for table in tables:
        sql = read_sql_file(f'{table}.sql','sql/')
        df = extract_data_to_sql(sql, f'{database}', env='prd')
        df = delete_flex_field_cols(df)
        df = mascarar_coluna_obs(df, 'OBS_ALUNO_FINAN')
        df = upper_columns_values(df, 'PAIS_2G')
        dataframe_to_parquet(df, f'{table}_{database}')


