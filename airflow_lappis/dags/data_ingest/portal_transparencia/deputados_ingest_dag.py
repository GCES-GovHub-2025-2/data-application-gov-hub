import logging
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_deputados import ClienteDeputados
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "Leonardo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["camara_deputados", "deputados", "dados_abertos"],
)
def deputados_ingest_dag() -> None:
    """
    DAG para buscar e armazenar dados de deputados da Câmara dos Deputados.
    """

    @task
    def fetch_and_store_deputados() -> dict:
        """
        Busca e armazena dados básicos dos deputados ativos.
        Itera por múltiplas páginas até não haver mais dados.
        """
        logging.info("Iniciando extração de deputados")

        filtros_config_str = Variable.get("deputados_filtros", default_var="{}")
        filtros_config = yaml.safe_load(filtros_config_str)

        api = ClienteDeputados()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        try:
            todos_deputados = []
            pagina = 1
            max_paginas = 100
            itens_por_pagina = 100

            logging.info("Iniciando coleta paginada de deputados...")

            # Percorre por toda a paginação disponível
            while pagina <= max_paginas:
                params = {"pagina": pagina, "itens": itens_por_pagina, **filtros_config}

                logging.info(f"Buscando página {pagina} com parâmetros: {params}")
                deputados = api.get_deputados(**params)

                # Failsafe para encerrar se não houver mais dados
                if not deputados or len(deputados) == 0:
                    logging.info(f"Nenhum dado na página {pagina}. Finalizando coleta.")
                    break

                logging.info(f"Página {pagina}: {len(deputados)} deputados recebidos")
                todos_deputados.extend(deputados)

                # Localiza a ultima pagina
                if len(deputados) < itens_por_pagina:
                    logging.info(
                        f"Última página detectada "
                        f"(retornou {len(deputados)} < {itens_por_pagina})"
                    )
                    break

                pagina += 1

            logging.info(f"Total de deputados coletados: {len(todos_deputados)}")

            # Insere no bd
            if todos_deputados:
                for deputado in todos_deputados:
                    deputado["dt_ingest"] = datetime.now().isoformat()

                logging.info(f"Inserindo {len(todos_deputados)} deputados no banco")
                db.insert_data(
                    todos_deputados,
                    "deputados",
                    conflict_fields=["id"],
                    primary_key=["id"],
                    schema="camara_deputados",
                )

                return {
                    "total_deputados": len(todos_deputados),
                    "paginas_processadas": pagina - 1,
                    "filtros": filtros_config,
                }
            else:
                logging.warning("Nenhum deputado encontrado")
                return {
                    "total_deputados": 0,
                    "paginas_processadas": 0,
                    "filtros": filtros_config,
                }

        except Exception as e:
            logging.error(f"Erro ao buscar/inserir deputados: {e}")
            raise

    fetch_and_store_deputados()


dag_instance = deputados_ingest_dag()
