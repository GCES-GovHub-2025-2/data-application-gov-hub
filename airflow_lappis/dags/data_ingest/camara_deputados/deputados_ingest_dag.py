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
    def fetch_and_store_legislaturas() -> None:
        """
        Busca e armazena informações sobre legislaturas.
        """
        logging.info("Iniciando extração de legislaturas")

        api = ClienteDeputados()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        legislaturas = api.get_legislaturas()

        if legislaturas:
            # Adicionar timestamp de ingestão
            for legislatura in legislaturas:
                legislatura["dt_ingest"] = datetime.now().isoformat()

            logging.info(f"Inserindo {len(legislaturas)} legislaturas no banco")
            db.insert_data(
                legislaturas,
                "legislaturas",
                conflict_fields=["id"],
                primary_key=["id"],
                schema="camara_deputados",
            )
        else:
            logging.warning("Nenhuma legislatura encontrada")

    @task
    def fetch_and_store_deputados() -> dict:
        """
        Busca e armazena dados básicos dos deputados ativos.

        """
        logging.info("Iniciando extração de deputados")

        filtros_config_str = Variable.get("deputados_filtros", default_var="{}")
        filtros_config = yaml.safe_load(filtros_config_str)

        api = ClienteDeputados()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        max_deputados = Variable.get("deputados_max_itens", default_var="600")
        params = {"itens": int(max_deputados), **filtros_config}

        logging.info(f"Buscando deputados com parâmetros: {params}")
        deputados = api.get_deputados(**params)

        if deputados:
            for deputado in deputados:
                deputado["dt_ingest"] = datetime.now().isoformat()

            logging.info(f"Inserindo {len(deputados)} deputados no banco")
            db.insert_data(
                deputados,
                "deputados",
                conflict_fields=["id"],
                primary_key=["id"],
                schema="camara_deputados",
            )

            return {"total_deputados": len(deputados), "filtros": filtros_config}
        else:
            logging.warning("Nenhum deputado encontrado")
            return {"total_deputados": 0, "filtros": filtros_config}

    legislaturas = fetch_and_store_legislaturas()
    deputados_stats = fetch_and_store_deputados()

    [legislaturas, deputados_stats]


dag_instance = deputados_ingest_dag()
