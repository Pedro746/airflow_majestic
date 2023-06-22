from airflow import DAG 
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd



def read_csv_and_insert_to_postgres():
    # Configurações do arquivo
    file_path = '/opt/airflow/data/cliente.xlsx'

    # Nome das colunas
    columns_names = ['id_venda'	'id_nota'	'data'	'frente'	'desconto'	'vendedor'	'cliente'	'produto'	'quantidade'	'filial'	'forma_de_pagamento'	'data_de_faturamento']

    # Lendo o arquivo
    df = pd.read_excel(file_path, names = columns_names)

    # Conectando ao banco de dados PostgreSQL
    postgres_conn_id = 'conn_db_majestic'
    postgres_hook = PostgresHook(postgres_conn_id)

    # Inserindo os dados no banco de dados
    postgres_hook.insert_rowns(table = 'cliente', rows = df.values.tolist())