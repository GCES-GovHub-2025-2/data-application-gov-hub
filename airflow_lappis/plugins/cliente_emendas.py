import http
from typing import Optional

from cliente_base import ClienteBase


class ClienteEmendas(ClienteBase):

    BASE_URL = "https://api.portaldatransparencia.gov.br" 

    def __init__(self) -> None:
        super().__init__(base_url=ClienteEmendas.BASE_URL)

    def get_emendas(
        self,
        codigoEmenda: Optional[str] = None,
        numeroEmenda: Optional[str] = None,
        nomeAutor: Optional[str] = None,
        tipoEmenda: Optional[str] = None,
        ano: Optional[int] = None,
        codigoFuncao: Optional[str] = None,
        codigoSubfuncao: Optional[str] = None,
        pagina: int = 1,
    ) -> Optional[list]:
        """
        Consultar Emendas Parlamentares.

        Args:
            codigoEmenda (Optional[str]): Código da emenda
            numeroEmenda (Optional[str]): Número da emenda
            nomeAutor (Optional[str]): Nome do autor
            tipoEmenda (Optional[str]): Tipo da emenda
            ano (Optional[int]): Ano da emenda
            codigoFuncao (Optional[str]): Código da função
            codigoSubfuncao (Optional[str]): Código da subfunção
            pagina (int): Página da consulta (padrão = 1)

        Returns:
            list: Lista de emendas parlamentares.
        """
        endpoint = "/api-de-dados/emendas"
        params = {"pagina": pagina}

        if codigoEmenda:
            params["codigoEmenda"] = codigoEmenda
        if numeroEmenda:
            params["numeroEmenda"] = numeroEmenda
        if nomeAutor:
            params["nomeAutor"] = nomeAutor
        if tipoEmenda:
            params["tipoEmenda"] = tipoEmenda
        if ano:
            params["ano"] = ano
        if codigoFuncao:
            params["codigoFuncao"] = codigoFuncao
        if codigoSubfuncao:
            params["codigoSubfuncao"] = codigoSubfuncao

        headers = {"accept": "*/*"}

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, params=params, headers=headers
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            return data
        return None
