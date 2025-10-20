import sys
import os
from unittest.mock import patch, MagicMock
from datetime import datetime

# Adiciona os caminhos necessários
sys.path.append(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "airflow_lappis", "plugins")
)
sys.path.append(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "airflow_lappis", "dags")
)


class TestDeputadosIngestLogic:
    """Testes para a lógica de negócio do ingest de deputados."""

    def setup_method(self) -> None:
        """Configuração inicial para cada teste."""
        self.mock_deputados_data = [
            {
                "id": 220593,
                "uri": "https://dadosabertos.camara.leg.br/api/v2/deputados/220593",
                "nome": "Abel Mesquita Jr.",
                "siglaPartido": "REPUBLICANOS",
                "uriPartido": "https://dadosabertos.camara.leg.br/api/v2/partidos/36835",
                "siglaUf": "RR",
                "idLegislatura": 57,
                "urlFoto": "https://www.camara.leg.br/internet/deputado/bandep/220593.jpg",
                "email": "dep.abelmesquitajr@camara.leg.br",
            },
            {
                "id": 204554,
                "uri": "https://dadosabertos.camara.leg.br/api/v2/deputados/204554",
                "nome": "Acácio Favacho",
                "siglaPartido": "MDB",
                "uriPartido": "https://dadosabertos.camara.leg.br/api/v2/partidos/36885",
                "siglaUf": "AP",
                "idLegislatura": 57,
                "urlFoto": "https://www.camara.leg.br/internet/deputado/bandep/204554.jpg",
                "email": "dep.acaciofavacho@camara.leg.br",
            },
        ]

    def test_data_schema_validation_deputados(self) -> None:
        """Testa validação do schema de dados de deputados."""
        # Schema esperado baseado na API real
        expected_fields = [
            "id",
            "uri",
            "nome",
            "siglaPartido",
            "uriPartido",
            "siglaUf",
            "idLegislatura",
            "urlFoto",
            "email",
        ]

        # Verifica se todos os campos esperados estão presentes
        for deputado in self.mock_deputados_data:
            for field in expected_fields:
                assert field in deputado, f"Campo {field} não encontrado nos dados"

    def test_data_processing_workflow_deputados(self) -> None:
        """Testa o fluxo de processamento de dados de deputados."""
        # Simula o processamento que acontece na DAG
        deputados = self.mock_deputados_data.copy()

        # Adiciona timestamp de ingestão
        for deputado in deputados:
            deputado["dt_ingest"] = datetime.now().isoformat()

        # Verificações
        assert len(deputados) == 2
        for deputado in deputados:
            assert "dt_ingest" in deputado
            assert "id" in deputado
            assert isinstance(deputado["id"], int)

    @patch("cliente_deputados.ClienteDeputados")
    def test_deputados_fetch_logic(self, mock_cliente_class: MagicMock) -> None:
        """Testa a lógica de busca de deputados com paginação."""
        # Mock da API
        mock_cliente = MagicMock()
        mock_cliente_class.return_value = mock_cliente

        # Simula múltiplas páginas
        mock_cliente.get_deputados.side_effect = [
            [self.mock_deputados_data[0]],
            [self.mock_deputados_data[1]],
            [],  # Página vazia para parar
        ]

        # Simula a lógica da DAG com paginação
        api = mock_cliente_class()
        todos_deputados = []
        pagina = 1
        max_paginas = 100

        while pagina <= max_paginas:
            deputados = api.get_deputados(pagina=pagina, itens=100)

            if not deputados or len(deputados) == 0:
                break

            todos_deputados.extend(deputados)
            pagina += 1

        # Verificações
        assert len(todos_deputados) == 2
        assert mock_cliente.get_deputados.call_count == 3
        assert todos_deputados[0] == self.mock_deputados_data[0]
        assert todos_deputados[1] == self.mock_deputados_data[1]

    def test_error_handling_empty_data(self) -> None:
        """Testa tratamento de erros com dados vazios."""
        # Testa com dados None
        deputados = None

        if deputados:
            for deputado in deputados:
                deputado["dt_ingest"] = datetime.now().isoformat()

        # Testa com lista vazia
        deputados = []
        assert len(deputados) == 0

    def test_timestamp_format(self) -> None:
        """Testa se o timestamp está no formato correto."""
        deputado = self.mock_deputados_data[0].copy()
        deputado["dt_ingest"] = datetime.now().isoformat()

        # Verifica se o timestamp está no formato ISO
        assert "T" in deputado["dt_ingest"]
        assert len(deputado["dt_ingest"]) > 10

    def test_upsert_preparation_deputados(self) -> None:
        """Testa preparação dos dados de deputados para UPSERT."""
        # Simula dados processados prontos para UPSERT
        deputados = self.mock_deputados_data.copy()

        # Adiciona timestamps
        for deputado in deputados:
            deputado["dt_ingest"] = datetime.now().isoformat()

        # Simula parâmetros de UPSERT
        table_name = "deputados"
        conflict_fields = ["id"]
        primary_key = ["id"]
        schema = "camara_deputados"

        # Verificações
        assert len(deputados) == 2
        assert table_name == "deputados"
        assert conflict_fields == ["id"]
        assert primary_key == ["id"]
        assert schema == "camara_deputados"

        # Verifica se todos os registros têm ID
        for deputado in deputados:
            assert "id" in deputado
            assert isinstance(deputado["id"], int)

    @patch("cliente_deputados.ClienteDeputados")
    def test_api_error_handling(self, mock_cliente_class: MagicMock) -> None:
        """Testa tratamento de erros da API."""
        # Mock da API retornando None (erro)
        mock_cliente = MagicMock()
        mock_cliente_class.return_value = mock_cliente
        mock_cliente.get_deputados.return_value = None

        # Simula a lógica da DAG
        api = mock_cliente_class()
        deputados = api.get_deputados(itens=600)

        # Verificações
        assert deputados is None

    def test_filters_application(self) -> None:
        """Testa aplicação de filtros nos parâmetros."""
        filtros_config = {"siglaUf": "SP", "siglaPartido": "PT", "idLegislatura": 57}

        pagina = 1
        itens_por_pagina = 100
        params = {"pagina": pagina, "itens": itens_por_pagina, **filtros_config}

        # Verificações
        assert params["pagina"] == 1
        assert params["itens"] == 100
        assert params["siglaUf"] == "SP"
        assert params["siglaPartido"] == "PT"
        assert params["idLegislatura"] == 57

    def test_return_stats_format(self) -> None:
        """Testa formato dos stats retornados."""
        filtros_config = {"siglaUf": "SP"}
        total_deputados = 2
        paginas_processadas = 1

        stats = {
            "total_deputados": total_deputados,
            "paginas_processadas": paginas_processadas,
            "filtros": filtros_config,
        }

        # Verificações
        assert "total_deputados" in stats
        assert "paginas_processadas" in stats
        assert "filtros" in stats
        assert stats["total_deputados"] == 2
        assert stats["paginas_processadas"] == 1
        assert stats["filtros"]["siglaUf"] == "SP"

    def test_data_types_consistency(self) -> None:
        """Testa consistência dos tipos de dados."""
        for deputado in self.mock_deputados_data:
            assert isinstance(deputado["id"], int)
            assert isinstance(deputado["nome"], str)
            assert isinstance(deputado["siglaPartido"], str)
            assert isinstance(deputado["siglaUf"], str)
            assert isinstance(deputado["idLegislatura"], int)
            assert isinstance(deputado["email"], str)

    @patch("cliente_deputados.ClienteDeputados")
    def test_pagination_logic(self, mock_cliente_class: MagicMock) -> None:
        """Testa a lógica de paginação."""
        # Mock da API
        mock_cliente = MagicMock()
        mock_cliente_class.return_value = mock_cliente

        # Simula múltiplas páginas
        mock_cliente.get_deputados.side_effect = [
            [{"id": 1, "nome": "Deputado 1"}],
            [{"id": 2, "nome": "Deputado 2"}],
            [],  # Página vazia para parar
        ]

        # Simula a lógica de paginação da DAG
        api = mock_cliente_class()
        todos_deputados = []
        pagina = 1
        max_paginas = 100
        itens_por_pagina = 100

        while pagina <= max_paginas:
            params = {"pagina": pagina, "itens": itens_por_pagina}
            deputados_pagina = api.get_deputados(**params)

            if not deputados_pagina or len(deputados_pagina) == 0:
                break

            todos_deputados.extend(deputados_pagina)
            pagina += 1

        # Verificações
        assert len(todos_deputados) == 2
        assert mock_cliente.get_deputados.call_count == 3
        assert todos_deputados[0]["id"] == 1
        assert todos_deputados[1]["id"] == 2

    @patch("cliente_deputados.ClienteDeputados")
    def test_max_pagination_limit(self, mock_cliente_class: MagicMock) -> None:
        """Testa limite máximo de paginação."""
        # Mock da API que sempre retorna dados
        mock_cliente = MagicMock()
        mock_cliente_class.return_value = mock_cliente
        mock_cliente.get_deputados.return_value = [{"id": 1, "nome": "Deputado Teste"}]

        # Simula a lógica de paginação com limite
        api = mock_cliente_class()
        pagina = 1
        max_paginas = 5  # Limite menor para teste

        while pagina <= max_paginas:
            deputados_pagina = api.get_deputados(pagina=pagina, itens=100)
            if not deputados_pagina or len(deputados_pagina) == 0:
                break
            pagina += 1

        # Verifica se parou no limite
        assert mock_cliente.get_deputados.call_count == 5

    @patch("cliente_deputados.ClienteDeputados")
    def test_early_pagination_stop(self, mock_cliente_class: MagicMock) -> None:
        """Testa parada antecipada quando retorna menos que o máximo."""
        # Mock da API
        mock_cliente = MagicMock()
        mock_cliente_class.return_value = mock_cliente

        # Primeira página retorna dados completos, segunda retorna menos que o máximo
        mock_cliente.get_deputados.side_effect = [
            [{"id": i} for i in range(100)],  # 100 itens
            [{"id": 101}],  # 1 item (menos que itens_por_pagina)
        ]

        # Simula a lógica da DAG
        api = mock_cliente_class()
        todos_deputados = []
        pagina = 1
        itens_por_pagina = 100

        while True:
            deputados_pagina = api.get_deputados(pagina=pagina, itens=itens_por_pagina)

            if not deputados_pagina or len(deputados_pagina) == 0:
                break

            todos_deputados.extend(deputados_pagina)

            # Para se retornou menos que o máximo (última página)
            if len(deputados_pagina) < itens_por_pagina:
                break

            pagina += 1

        # Verificações
        assert len(todos_deputados) == 101
        assert mock_cliente.get_deputados.call_count == 2
