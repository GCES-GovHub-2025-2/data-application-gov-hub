import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook

# Arquivo pra dar uma olhada: cargos_funcao_ingest_dag.py
# TODO: Rever numero de paginas da requisição
# TODO: Usar classes modularizadas (Criar conexão com airflow, ingestão de json)
# PARA CONEXÃO USAR (EXEMPLO): from cliente_postgres import ClientPostgresDB
# PARA CONEXÃO USAR (EXEMPLO): from postgres_helpers import get_postgres_conn
# PARA CONSULTAS USAR (EXEMPLO:): from cliente_postgres import ClientPostgresDB



API_KEY = "INSIRA AQUI SUA CHAVE"
BASE_URL = "https://api.portaldatransparencia.gov.br/api-de-dados/emendas"
POSTGRES_CONN_ID = "postgres_default"
NOME_TABELA = "emendas"

# Task 1 - Requisição API
def request_emendas(**context):
    url = f"{BASE_URL}?pagina=1"
    headers = {
        "accept": "*/*",
        "chave-api-dados": API_KEY
    }

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code != 200:
        raise Exception(f"Erro na API: {response.status_code}, {response.text}")

    data = response.json()
    logging.info(f"Total registros recebidos: {len(data)}")

    # envia os dados pro XCom
    context["ti"].xcom_push(key="emendas_data", value=data)


# Task 2 - Ingestão no banco
def emendas_ingestion(**context):
    data = context["ti"].xcom_pull(task_ids="request_emendas", key="emendas_data")

    if not data:
        logging.warning("Nenhum dado para inserir.")
        return

    # pega conexão configurada no Airflow
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # cria a tabela caso não exista
    cur.execute("""
        CREATE TABLE IF NOT EXISTS emendas (
            codigoEmenda TEXT,
            ano INT,
            tipoEmenda TEXT,
            autor TEXT,
            nomeAutor TEXT,
            numeroEmenda TEXT,
            localidadeDoGasto TEXT,
            funcao TEXT,
            subfuncao TEXT,
            valorEmpenhado TEXT,
            valorLiquidado TEXT,
            valorPago TEXT,
            valorRestoInscrito TEXT,
            valorRestoCancelado TEXT,
            valorRestoPago TEXT
        );
    """)

    # insere os registros
    for row in data:
        cur.execute("""
            INSERT INTO emendas (
                codigoEmenda, ano, tipoEmenda, autor, nomeAutor, numeroEmenda, 
                localidadeDoGasto, funcao, subfuncao, valorEmpenhado, valorLiquidado, 
                valorPago, valorRestoInscrito, valorRestoCancelado, valorRestoPago
            ) VALUES (
                %(codigoEmenda)s, %(ano)s, %(tipoEmenda)s, %(autor)s, %(nomeAutor)s, %(numeroEmenda)s, 
                %(localidadeDoGasto)s, %(funcao)s, %(subfuncao)s, %(valorEmpenhado)s, %(valorLiquidado)s, 
                %(valorPago)s, %(valorRestoInscrito)s, %(valorRestoCancelado)s, %(valorRestoPago)s
            );
        """, row)

    conn.commit()
    cur.close()
    conn.close()

    logging.info(f"Inseridos {len(data)} registros no banco.")


# DAG
with DAG(
    dag_id="api_emendas",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "owner": "Motoca",
    },
    tags=["emendas_parlamentares", "api"],
) as dag:

    t1 = PythonOperator(
        task_id="request_emendas",
        python_callable=request_emendas,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="emendas_ingestion",
        python_callable=emendas_ingestion,
        provide_context=True,
    )

    t1 >> t2
