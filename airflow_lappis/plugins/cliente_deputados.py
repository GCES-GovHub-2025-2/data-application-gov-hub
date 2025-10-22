import http
import logging
from typing import Any
from cliente_base import ClienteBase


class ClienteDeputados(ClienteBase):
    """
    Cliente para consumir a API de Dados Abertos da Câmara dos Deputados.
    """

    BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteDeputados.BASE_URL)
        logging.info(
            "[cliente_deputados.py] Initialized ClienteDeputados with base_url: "
            f"{ClienteDeputados.BASE_URL}"
        )

    def get_deputados(self, **params: Any) -> list[dict[str, Any]] | None:
        """
        Obter lista de deputados
        """
        endpoint = "/deputados"
        logging.info(f"[cliente_deputados.py] Fetching deputados with params: {params}")

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER, params=params
        )

        if status == http.HTTPStatus.OK and isinstance(data, dict):
            deputados: list[dict[str, Any]] = data.get("dados", [])
            logging.info(
                f"[cliente_deputados.py] Successfully fetched {len(deputados)} deputados"
            )
            return deputados
        else:
            logging.warning(
                f"[cliente_deputados.py] Failed to fetch deputados with status: {status}"
            )
            return None
