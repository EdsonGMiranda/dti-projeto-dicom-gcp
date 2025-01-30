import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus

def listar_drivers():
    """Lista todos os drivers ODBC disponíveis no sistema."""
    drivers = [driver for driver in pyodbc.drivers()]
    print("Drivers ODBC disponíveis:", drivers)
    return drivers


def conectar_sql_server(driver, server, database, username, password):
    """
    Função para se conectar a um SQL Server.

    :param driver: Nome do driver ODBC para SQL Server
    :param server: Nome ou IP do servidor SQL Server
    :param database: Nome do banco de dados
    :param username: Nome de usuário para autenticação
    :param password: Senha para autenticação
    :return: Engine de conexão com o banco de dados
    """
    parametros = (
        f'DRIVER={{{driver}}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        'TrustServerCertificate=yes;'
        'ENCRYPT=no;'
        f'UID={username};'
        f'PWD={password}'
    )
    url_db = quote_plus(parametros)
    engine = sal.create_engine(f'mssql+pyodbc:///?odbc_connect={url_db}')

    try:
        with engine.connect() as conn:
            query = "SELECT @@VERSION;"
            resultant = conn.execute(text(query))
            print('\n\n----------- Connection successful!')
            print(engine)
            for row in resultant:
                print(row)
    except Exception as e:
        print('\n\n----------- Connection failed! ERROR:', e)
        print(engine)
        return None

    return engine

def inspect_database(engine):
    """
    Função para inspecionar o banco de dados conectado.

    :param engine: Engine de conexão com o banco de dados
    :return: Objeto Inspector
    """
    inspector = inspect(engine)
    return inspector


