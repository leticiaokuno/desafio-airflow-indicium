from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd
import os

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['leticia.okuno@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# INSTRUÇÃO 1: Função que lê os dados da tabela 'Order' e salva como CSV
def export_orders_to_csv():
    db_path = 'data/Northwind_small.sqlite'
    conn = sqlite3.connect(db_path)  # Conecta ao banco SQLite
    query = "SELECT * FROM 'Order'"  # Query para selecionar todos os dados da tabela

    # Executa a query e transforma o resultado em um DataFrame do Pandas
    df = pd.read_sql_query(query, conn)

    # Exporta para um arquivo CSV
    df.to_csv('output_orders.csv', index=False)

    conn.close()  # Fecha a conexão com o banco


# INSTRUÇÃO 2: Calcular a soma da quantidade vendida para o RJ, lendo "OrderDetail" e fazendo um JOIN com "output_orders.csv"
def calculate_quantity_for_rio():
    # Caminhos para o banco de dados e CSV
    db_path = 'data/Northwind_small.sqlite'
    orders_csv = 'output_orders.csv'
    
    # Conecta ao banco SQLite
    conn = sqlite3.connect(db_path)
    
    # Lê a tabela 'OrderDetail' do banco de dados
    order_details_df = pd.read_sql_query("SELECT * FROM OrderDetail", conn)
    
    # Lê o arquivo CSV exportado na task anterior
    orders_df = pd.read_csv(orders_csv)

    # Converte as colunas 'OrderID' para string em ambos os dataframes
    order_details_df['Id'] = order_details_df['Id'].astype(str)
    orders_df['Id'] = orders_df['Id'].astype(str)
    
    # Realiza o JOIN entre as duas tabelas usando 'OrderID'
    merged_df = pd.merge(order_details_df, orders_df, on='Id')
    
    # Filtra as vendas para o Rio de Janeiro
    rio_sales = merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']
    
    # Calcula a soma da quantidade vendida (Quantity)
    total_quantity = rio_sales['Quantity'].sum()
    
    # Salva o valor em 'count.txt'
    with open('count.txt', 'w') as f:
        f.write(str(total_quantity))  # Converte para texto usando str()

    # Fecha a conexão com o banco de dados
    conn.close()





## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##



with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

  




    # Task para exportar dados de 'Order' para CSV
    export_orders_task = PythonOperator(
        task_id='export_orders_to_csv',
        python_callable=export_orders_to_csv,
    )
    export_orders_task.doc_md = dedent(
        """\
        #### Task Documentation
        Essa task lê os dados da tabela 'Order' e escreve um arquivo CSV chamado 'output_orders.csv'.
        """
    )

    # Task para calcular a soma da quantidade vendida para o RJ
    calculate_quantity_task = PythonOperator(
        task_id='calculate_quantity_for_rio',
        python_callable=calculate_quantity_for_rio,
    )



    # Ordem de execução das Tasks
    export_orders_task >> calculate_quantity_task >> export_final_output