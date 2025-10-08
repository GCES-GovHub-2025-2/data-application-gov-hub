import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_emendas import ClienteEmendas
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "owner": "Motoca",
    },
    tags=["emendas_parlamentares", "api"],
)
def api_emendas_dag() -> None:
    @task
    def fetch_and_store_emendas() -> None:
        """
        Busca emendas parlamentares da API e armazena no PostgreSQL.
        Itera por múltiplas páginas até não haver mais dados.
        """
        api = ClienteEmendas()
        db = ClientPostgresDB(get_postgres_conn())
        
        try:
            registros = []
            pagina = 1
            max_paginas = 100 
            
            logging.info("Iniciando coleta de emendas parlamentares...")
            
            # Itera por todas as páginas disponíveis
            while pagina <= max_paginas:
                logging.info(f"Buscando página {pagina}...")
                
                emendas_data = api.get_emendas(pagina=pagina)
                
                # Se não retornou dados ou retornou lista vazia, encerra
                if not emendas_data or len(emendas_data) == 0:
                    logging.info(f"Nenhum dado na página {pagina}. Finalizando coleta.")
                    break
                
                logging.info(f"Página {pagina}: {len(emendas_data)} registros recebidos")
                
                # Adiciona os registros à lista
                registros.extend(emendas_data)
                
                pagina += 1
            
            logging.info(f"Total de registros coletados: {len(registros)}")
            
            # Insere os dados no banco
            if registros:
                db.insert_data(
                    registros,
                    "emendas",
                    schema="public",
                )
                logging.info(f"Inseridos {len(registros)} registros no banco.")
            else:
                logging.warning("Nenhuma emenda encontrada para inserir.")
                
        except Exception as e:
            logging.error(f"Erro ao buscar/inserir emendas: {e}")
            raise

    fetch_and_store_emendas()


dag_instance = api_emendas_dag()