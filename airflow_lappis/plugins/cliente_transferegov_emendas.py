import http
import logging
from typing import Optional
from cliente_base import ClienteBase


class ClienteTransfereGovEmendas(ClienteBase):
    BASE_URL = "https://api.transferegov.gestao.gov.br/transferenciasespeciais/"
    BASE_HEADER = {"accept": "application/json"}

    def __init__(self) -> None:
        super().__init__(base_url=ClienteTransfereGovEmendas.BASE_URL)

    def get_transferencias_especiais_by_id_plano_acao(
        self,
        id_plano_acao: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Optional[list]:
        """
        Busca transferências especiais a partir do ID do plano de ação.
        Segue o padrão PostgREST usado pela API do TransfereGov.
        
        Args:
            id_plano_acao (str): ID do plano de ação
            limit (Optional[int]): Número máximo de registros a retornar (opcional)
            offset (Optional[int]): Número de registros a pular para paginação (opcional)
        
        Returns:
            Optional[list]: Lista de transferências especiais ou None em caso de erro
        """
        # Formato PostgREST: query string diretamente no endpoint
        endpoint = f"transferencia_especial?id_plano_acao=eq.{id_plano_acao}"
        
        # Adiciona parâmetros de paginação diretamente na query string (PostgREST)
        if limit:
            endpoint += f"&limit={limit}"
        if offset:
            endpoint += f"&offset={offset}"

        logging.info(
            f"[cliente_transferegov_emendas.py] Buscando transferências especiais "
            f"para id_plano_acao: {id_plano_acao}"
        )

        status, data = self.request(
            http.HTTPMethod.GET, endpoint, headers=self.BASE_HEADER
        )

        if status == http.HTTPStatus.OK and isinstance(data, list):
            logging.info(
                f"[cliente_transferegov_emendas.py] Sucesso ao buscar transferências "
                f"especiais para id_plano_acao: {id_plano_acao}. "
                f"Total de registros: {len(data)}"
            )
            return data
        else:
            logging.warning(
                f"[cliente_transferegov_emendas.py] Falha ao buscar transferências "
                f"especiais para id_plano_acao: {id_plano_acao}. Status: {status}"
            )
            return None

    def get_all_transferencias_especiais_by_id_plano_acao(
        self,
        id_plano_acao: str,
        limite_por_pagina: int = 100,
        max_paginas: int = 1000,
    ) -> list:
        """
        Busca todas as transferências especiais de um plano de ação com paginação automática.
        Itera por todas as páginas até não haver mais dados.
        
        Args:
            id_plano_acao (str): ID do plano de ação
            limite_por_pagina (int): Número de registros por página (padrão = 100)
            max_paginas (int): Número máximo de páginas para buscar (padrão = 1000)
        
        Returns:
            list: Lista com todas as transferências especiais encontradas
        """
        todas_transferencias = []
        offset = 0

        logging.info(
            f"[cliente_transferegov_emendas.py] Iniciando busca paginada de transferências "
            f"especiais para id_plano_acao: {id_plano_acao}"
        )

        for pagina in range(1, max_paginas + 1):
            logging.info(
                f"[cliente_transferegov_emendas.py] Buscando página {pagina} de "
                f"transferências especiais para id_plano_acao: {id_plano_acao}"
            )

            transferencias = self.get_transferencias_especiais_by_id_plano_acao(
                id_plano_acao=id_plano_acao,
                limit=limite_por_pagina,
                offset=offset,
            )

            # Se não retornou dados ou retornou lista vazia, encerra
            if not transferencias or len(transferencias) == 0:
                logging.info(
                    f"[cliente_transferegov_emendas.py] Nenhum dado na página {pagina}. "
                    f"Finalizando busca paginada."
                )
                break

            todas_transferencias.extend(transferencias)
            logging.info(
                f"[cliente_transferegov_emendas.py] Página {pagina}: {len(transferencias)} "
                f"registros recebidos. Total acumulado: {len(todas_transferencias)}"
            )

            # Se retornou menos registros que o limite, provavelmente é a última página
            if len(transferencias) < limite_por_pagina:
                logging.info(
                    f"[cliente_transferegov_emendas.py] Página {pagina} contém menos "
                    f"registros que o limite ({len(transferencias)} < {limite_por_pagina}). "
                    f"Finalizando busca paginada."
                )
                break

            offset += limite_por_pagina

        logging.info(
            f"[cliente_transferegov_emendas.py] Busca paginada concluída. "
            f"Total de transferências especiais encontradas: {len(todas_transferencias)}"
        )

        return todas_transferencias

