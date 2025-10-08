import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Adiciona o caminho dos plugins ao sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'airflow_lappis', 'plugins'))

from cliente_emendas import ClienteEmendas


class TestClienteEmendas:
    """Testes para a classe ClienteEmendas."""

    def setup_method(self) -> None:
        """Configuração inicial para cada teste."""
        self.cliente = ClienteEmendas()

    def test_init(self) -> None:
        """Testa inicialização do cliente."""
        assert self.cliente.base_url == "https://api.portaldatransparencia.gov.br"
        assert self.cliente.API_KEY is not None

    @patch('cliente_emendas.ClienteBase.request')
    def test_get_emendas_success(self, mock_request: MagicMock) -> None:
        """Testa busca de emendas com sucesso."""
        # Mock da resposta da API
        mock_data = [
            {
                "codigoEmenda": "201826760002",
                "ano": 2018,
                "tipoEmenda": "Emenda Individual",
                "autor": "VINICIUS GURGEL",
                "nomeAutor": "VINICIUS GURGEL",
                "numeroEmenda": "0002",
                "localidadeDoGasto": "AMAPÁ (UF)",
                "funcao": "Segurança pública",
                "subfuncao": "Policiamento",
                "valorEmpenhado": "0,00",
                "valorLiquidado": "0,00",
                "valorPago": "0,00"
            }
        ]
        mock_request.return_value = (200, mock_data)

        result = self.cliente.get_emendas(pagina=1)

        assert result == mock_data
        mock_request.assert_called_once()
        call_args = mock_request.call_args
        assert call_args[1]['params']['pagina'] == 1

    @patch('cliente_emendas.ClienteBase.request')
    def test_get_emendas_with_filters(self, mock_request: MagicMock) -> None:
        """Testa busca de emendas com filtros."""
        mock_request.return_value = (200, [])

        self.cliente.get_emendas(
            codigoEmenda="201826760002",
            nomeAutor="VINICIUS GURGEL",
            ano=2018,
            pagina=2
        )

        call_args = mock_request.call_args
        params = call_args[1]['params']
        
        assert params['codigoEmenda'] == "201826760002"
        assert params['nomeAutor'] == "VINICIUS GURGEL"
        assert params['ano'] == 2018
        assert params['pagina'] == 2

    @patch('cliente_emendas.ClienteBase.request')
    def test_get_emendas_api_error(self, mock_request: MagicMock) -> None:
        """Testa comportamento quando API retorna erro."""
        mock_request.return_value = (400, {"error": "Bad Request"})

        result = self.cliente.get_emendas(pagina=1)

        assert result is None

    @patch('cliente_emendas.ClienteBase.request')
    def test_get_emendas_empty_response(self, mock_request: MagicMock) -> None:
        """Testa comportamento quando API retorna lista vazia."""
        mock_request.return_value = (200, [])

        result = self.cliente.get_emendas(pagina=1)

        assert result == []

    @patch('cliente_emendas.ClienteBase.request')
    def test_get_emendas_invalid_response_type(self, mock_request: MagicMock) -> None:
        """Testa comportamento quando API retorna tipo inválido."""
        mock_request.return_value = (200, {"not": "a_list"})

        result = self.cliente.get_emendas(pagina=1)

        assert result is None

    def test_api_key_headers(self) -> None:
        """Testa se a API key está sendo incluída nos headers."""
        with patch('cliente_emendas.ClienteBase.request') as mock_request:
            mock_request.return_value = (200, [])
            
            self.cliente.get_emendas(pagina=1)
            
            call_args = mock_request.call_args
            headers = call_args[1]['headers']
            
            assert 'chave-api-dados' in headers
            assert headers['accept'] == '*/*'

    @patch.dict(os.environ, {'PORTAL_TRANSPARENCIA_API_KEY': 'test_key'})
    def test_api_key_from_environment(self) -> None:
        """Testa se a API key é carregada do ambiente."""
        # Recarrega o módulo para pegar a nova variável de ambiente
        import importlib
        import cliente_emendas
        importlib.reload(cliente_emendas)
        
        cliente = cliente_emendas.ClienteEmendas()
        assert cliente.API_KEY == 'test_key'

    @patch('cliente_emendas.ClienteBase.request')
    def test_all_optional_parameters(self, mock_request: MagicMock) -> None:
        """Testa se todos os parâmetros opcionais são passados corretamente."""
        mock_request.return_value = (200, [])

        self.cliente.get_emendas(
            codigoEmenda="123",
            numeroEmenda="456",
            nomeAutor="TESTE",
            tipoEmenda="Individual",
            ano=2023,
            codigoFuncao="01",
            codigoSubfuncao="001",
            pagina=5
        )

        call_args = mock_request.call_args
        params = call_args[1]['params']
        
        expected_params = {
            'codigoEmenda': '123',
            'numeroEmenda': '456',
            'nomeAutor': 'TESTE',
            'tipoEmenda': 'Individual',
            'ano': 2023,
            'codigoFuncao': '01',
            'codigoSubfuncao': '001',
            'pagina': 5
        }
        
        assert params == expected_params
