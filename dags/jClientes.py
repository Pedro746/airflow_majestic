from airflow import DAG 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime
import pandas as pd



def read_csv_and_insert_to_postgres_clientes():

    # Configurações do arquivo
    file_path = '/opt/airflow/data/cliente.csv'

    # Lendo o arquivo
    df = pd.read_csv(file_path)

    #Renomeando as colunas
    df = df.rename(columns = {
        'idCliente' : 'id_cliente',
        'Nome' : 'nome',
        'Tipo' : 'tipo',
        'Media de Alunos' : 'media_de_alunos',
        'Classificacao' : 'classificacao',
        'idCidade' : 'id_cidade'
    })

    # Conectando ao banco de dados PostgreSQL
    postgres_conn_id = 'conn_db_majestic'
    postgres_hook = PostgresHook(postgres_conn_id)

    # Inserindo os dados no banco de dados
    postgres_hook.insert_rows(table = 'cliente', rows = df.values.tolist())


#  Função que realiza a contagem de linhas inseridas na tabela
def total_rows_inserted(ti):

    task_instance = ti.xcom_pull(task_ids = 'query_data_count')
    print(f'Total de linhas inseridas: {task_instance}')


# Definindo a DAG
dag = DAG(
    'jClientes',
    description= 'DAG que lê um arquivo e salva os dados numa tabela no banco de dados Postgres',
    schedule_interval= None,
    start_date= datetime(2023, 6, 22),
    catchup= False,
    default_view='graph',
    template_searchpath = ['/opt/airflow/sql', '/opt/airflow/template']
)

# Tarefa que executa a função de criação da tabela
create_table = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'conn_db_majestic',
    sql = 'cliente.sql',
    dag = dag
)

# Tarefa que executa um truncate na tabela caso já exista e tenha dados
truncate_table = PostgresOperator(
    task_id = 'truncate_table',
    postgres_conn_id = 'conn_db_majestic',
    sql = 'truncate_cliente.sql',
    dag = dag
)

# Tarefa que executa a função de leitura do arquivo e inserção no PostgreSQL
file_to_postgres = PythonOperator(
    task_id = 'file_to_postgres',
    python_callable = read_csv_and_insert_to_postgres_clientes,
    dag = dag
)

# Tarefa que faz uma contagem de linhas de dados na tabela
query_data_count = PostgresOperator(
    task_id = 'query_data_count',
    postgres_conn_id = 'conn_db_majestic',
    sql = 'count_registros_clientes.sql',
    dag = dag
)

# Tarefa que executa a função de contagem de linhas inseridas na tabela
print_total_rows_inserted = PythonOperator(
    task_id = 'print_total_rows_inserted',
    python_callable = total_rows_inserted,
    provide_context = True,
    dag = dag
)

# Tarefa que dispara um email caso ocorra algum erro
send_email_error = EmailOperator( 
    task_id='send_email_error',
    to= 'pedrolima4680@gmail.com',
    subject= 'Airflow Error',
    html_content= 'alert_errors.html',
    dag=dag,
    trigger_rule= 'one_failed'
    )

# Tarefa que dispara um email caso não ocorra nenhum erro
send_email_success = EmailOperator( 
    task_id='send_email_success',
    to= 'pedrolima4680@gmail.com',
    subject= 'Airflow Success',
    html_content= 'alert_success.html',
    dag=dag,
    trigger_rule= 'all_success'
    )

create_table >> truncate_table >> file_to_postgres >> query_data_count >> print_total_rows_inserted
print_total_rows_inserted >> send_email_success
print_total_rows_inserted >> send_email_error