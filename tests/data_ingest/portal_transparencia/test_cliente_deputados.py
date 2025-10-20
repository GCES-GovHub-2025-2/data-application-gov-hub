import os
import sys
from typing import Any
from unittest.mock import patch, MagicMock

# Adiciona o caminho dos plugins ao sys.path
sys.path.append(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "airflow_lappis", "plugins")
)

from cliente_deputados import ClienteDeputados


class TestClienteDeputados:
    """Testes para a classe ClienteDeputados."""

    def setup_method(self) -> None:
        """Configuração inicial para cada teste."""
        self.cliente = ClienteDeputados()

    def test_init(self) -> None:
        """Testa inicialização do cliente."""
        assert self.cliente.base_url == "https://dadosabertos.camara.leg.br/api/v2"

    @patch("cliente_deputados.ClienteBase.request")
    def test_get_deputados_success(self, mock_request: MagicMock) -> None:
        """Testa busca de deputados com sucesso."""
        # Mock da resposta da API
        mock_data = {
            "dados": [
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
                }
            ]
        }
        mock_request.return_value = (200, mock_data)

        result = self.cliente.get_deputados()

        assert result == mock_data["dados"]
        assert len(result) == 1
        assert result[0]["id"] == 220593
        mock_request.assert_called_once()

    @patch("cliente_deputados.ClienteBase.request")
    def test_get_deputados_with_filters(self, mock_request: MagicMock) -> None:
        """Testa busca de deputados com filtros."""
        mock_data: dict[str, list[Any]] = {"dados": []}
        mock_request.return_value = (200, mock_data)

        self.cliente.get_deputados(
            siglaUf="SP", siglaPartido="PT", idLegislatura=57, itens=100
        )

        call_args = mock_request.call_args
        params = call_args[1]["params"]

        assert params["siglaUf"] == "SP"
        assert params["siglaPartido"] == "PT"
        assert params["idLegislatura"] == 57
        assert params["itens"] == 100

    @patch("cliente_deputados.ClienteBase.request")
    def test_get_deputados_api_error(self, mock_request: MagicMock) -> None:
        """Testa comportamento quando API retorna erro."""
        mock_request.return_value = (400, {"error": "Bad Request"})

        result = self.cliente.get_deputados()

        assert result is None

    @patch("cliente_deputados.ClienteBase.request")
    def test_get_deputados_empty_response(self, mock_request: MagicMock) -> None:
        """Testa comportamento quando API retorna lista vazia."""
        mock_data: dict[str, list[Any]] = {"dados": []}
        mock_request.return_value = (200, mock_data)

        result = self.cliente.get_deputados()

        assert result == []

    def test_headers_format(self) -> None:
        """Testa se os headers estão no formato correto."""
        with patch("cliente_deputados.ClienteBase.request") as mock_request:
            mock_request.return_value = (200, {"dados": []})

            self.cliente.get_deputados()

            call_args = mock_request.call_args
            headers = call_args[1]["headers"]

            assert "accept" in headers
            assert headers["accept"] == "application/json"

    @patch("cliente_deputados.ClienteBase.request")
    def test_all_optional_parameters_deputados(self, mock_request: MagicMock) -> None:
        """Testa se todos os parâmetros opcionais são passados corretamente."""
        mock_request.return_value = (200, {"dados": []})

        self.cliente.get_deputados(
            idLegislatura=57,
            siglaUf="SP",
            siglaPartido="PT",
            siglaSexo="F",
            itens=50,
            ordenarPor="nome",
        )

        call_args = mock_request.call_args
        params = call_args[1]["params"]

        expected_params = {
            "idLegislatura": 57,
            "siglaUf": "SP",
            "siglaPartido": "PT",
            "siglaSexo": "F",
            "itens": 50,
            "ordenarPor": "nome",
        }

        assert params == expected_params

    @patch("cliente_deputados.ClienteBase.request")
    def test_invalid_response_type(self, mock_request: MagicMock) -> None:
        """Testa comportamento quando API retorna tipo inválido."""
        mock_request.return_value = (200, ["not", "a", "dict"])

        result = self.cliente.get_deputados()

        assert result is None
