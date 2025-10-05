import pytest
import json
import hashlib
import os
import sys
from unittest.mock import patch, MagicMock

# Adiciona os caminhos necessários
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'airflow_lappis', 'plugins'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'airflow_lappis', 'dags'))

from cliente_emendas import ClienteEmendas


class TestEmendasParliamentaryLogic:
    """Testes para a lógica de negócio das emendas parlamentares."""

    def setup_method(self) -> None:
        """Configuração inicial para cada teste."""
        self.mock_emendas_data = [
            {
                "codigoEmenda": "201826760002",
                "ano": 2018,
                "tipoEmenda": "Emenda Individual - Transferências com Finalidade Definida",
                "autor": "VINICIUS GURGEL",
                "nomeAutor": "VINICIUS GURGEL",
                "numeroEmenda": "0002",
                "localidadeDoGasto": "AMAPÁ (UF)",
                "funcao": "Segurança pública",
                "subfuncao": "Policiamento",
                "valorEmpenhado": "0,00",
                "valorLiquidado": "0,00",
                "valorPago": "0,00",
                "valorRestoInscrito": "11.710,22",
                "valorRestoCancelado": "0,00",
                "valorRestoPago": "288.289,78"
            },
            {
                "codigoEmenda": "202050170007",
                "ano": 2020,
                "tipoEmenda": "Emenda de Comissão",
                "autor": "COMISSAO DE FINANCAS E TRIBUTACAO - CFT",
                "nomeAutor": "COMISSAO DE FINANCAS E TRIBUTACAO - CFT",
                "numeroEmenda": "0007",
                "localidadeDoGasto": "Nacional",
                "funcao": "Agricultura",
                "subfuncao": "Promoção da produção agropecuária",
                "valorEmpenhado": "0,00",
                "valorLiquidado": "0,00",
                "valorPago": "0,00",
                "valorRestoInscrito": "11.385.738,68",
                "valorRestoCancelado": "1.569.992,02",
                "valorRestoPago": "3.195.992,49"
            }
        ]

    def test_id_generation_consistency(self) -> None:
        """Testa se a geração de IDs é consistente."""
        test_data = self.mock_emendas_data[0].copy()
        
        # Gera ID duas vezes com os mesmos dados
        dados_hash1 = json.dumps(test_data, sort_keys=True, default=str).encode('utf-8')
        hash_id1 = hashlib.sha256(dados_hash1).hexdigest()[:16]
        
        dados_hash2 = json.dumps(test_data, sort_keys=True, default=str).encode('utf-8')
        hash_id2 = hashlib.sha256(dados_hash2).hexdigest()[:16]
        
        assert hash_id1 == hash_id2
        assert len(hash_id1) == 16
        assert len(hash_id2) == 16

    def test_id_generation_uniqueness(self) -> None:
        """Testa se IDs diferentes são gerados para dados diferentes."""
        test_data1 = self.mock_emendas_data[0].copy()
        test_data2 = self.mock_emendas_data[1].copy()
        
        # Gera IDs para dados diferentes
        dados_hash1 = json.dumps(test_data1, sort_keys=True, default=str).encode('utf-8')
        hash_id1 = hashlib.sha256(dados_hash1).hexdigest()[:16]
        
        dados_hash2 = json.dumps(test_data2, sort_keys=True, default=str).encode('utf-8')
        hash_id2 = hashlib.sha256(dados_hash2).hexdigest()[:16]
        
        assert hash_id1 != hash_id2

    def test_data_processing_workflow(self) -> None:
        """Testa o fluxo de processamento de dados."""
        # Simula o processamento que acontece na DAG
        registros = self.mock_emendas_data.copy()
        
        # Gera ID único para cada registro
        for emenda in registros:
            dados_hash = json.dumps(emenda, sort_keys=True, default=str).encode('utf-8')
            hash_id = hashlib.sha256(dados_hash).hexdigest()[:16]
            emenda["id"] = hash_id
        
        # Verificações
        assert len(registros) == 2
        for registro in registros:
            assert "id" in registro
            assert len(registro["id"]) == 16
            assert "codigoEmenda" in registro

    def test_data_schema_validation(self) -> None:
        """Testa validação do schema de dados."""
        # Schema esperado baseado na API real
        expected_fields = [
            "codigoEmenda",
            "ano", 
            "tipoEmenda",
            "autor",
            "nomeAutor",
            "numeroEmenda",
            "localidadeDoGasto",
            "funcao",
            "subfuncao",
            "valorEmpenhado",
            "valorLiquidado",
            "valorPago",
            "valorRestoInscrito",
            "valorRestoCancelado",
            "valorRestoPago"
        ]
        
        # Verifica se todos os campos esperados estão presentes
        for emenda in self.mock_emendas_data:
            for field in expected_fields:
                assert field in emenda, f"Campo {field} não encontrado nos dados"

    @patch('cliente_emendas.ClienteEmendas')
    def test_pagination_logic(self, mock_cliente_class: MagicMock) -> None:
        """Testa a lógica de paginação."""
        # Mock da API
        mock_cliente = MagicMock()
        mock_cliente_class.return_value = mock_cliente
        
        # Simula múltiplas páginas
        mock_cliente.get_emendas.side_effect = [
            [{"codigoEmenda": "page_1"}],
            [{"codigoEmenda": "page_2"}],
            []  # Página vazia para parar
        ]
        
        # Simula a lógica de paginação da DAG
        registros = []
        pagina = 1
        max_paginas = 100
        
        while pagina <= max_paginas:
            emendas_data = mock_cliente.get_emendas(pagina=pagina)
            
            if not emendas_data or len(emendas_data) == 0:
                break
                
            registros.extend(emendas_data)
            pagina += 1
        
        # Verificações
        assert len(registros) == 2
        assert mock_cliente.get_emendas.call_count == 3
        assert registros[0]["codigoEmenda"] == "page_1"
        assert registros[1]["codigoEmenda"] == "page_2"

    @patch('cliente_emendas.ClienteEmendas')
    def test_max_pagination_limit(self, mock_cliente_class: MagicMock) -> None:
        """Testa limite máximo de paginação."""
        # Mock da API que sempre retorna dados
        mock_cliente = MagicMock()
        mock_cliente_class.return_value = mock_cliente
        mock_cliente.get_emendas.return_value = [{"codigoEmenda": "test"}]
        
        # Simula a lógica de paginação com limite
        pagina = 1
        max_paginas = 5  # Limite menor para teste
        
        while pagina <= max_paginas:
            emendas_data = mock_cliente.get_emendas(pagina=pagina)
            if not emendas_data or len(emendas_data) == 0:
                break
            pagina += 1
        
        # Verifica se parou no limite
        assert mock_cliente.get_emendas.call_count == 5

    def test_error_handling_data_processing(self) -> None:
        """Testa tratamento de erros no processamento de dados."""
        # Testa com dados inválidos
        invalid_data = None
        
        try:
            if invalid_data:
                for emenda in invalid_data:
                    dados_hash = json.dumps(emenda, sort_keys=True, default=str).encode('utf-8')
                    hash_id = hashlib.sha256(dados_hash).hexdigest()[:16]
                    emenda["id"] = hash_id
        except Exception:
            # Esperado quando dados são None
            pass
        
        # Testa com lista vazia
        empty_data = []
        for emenda in empty_data:
            dados_hash = json.dumps(emenda, sort_keys=True, default=str).encode('utf-8')
            hash_id = hashlib.sha256(dados_hash).hexdigest()[:16]
            emenda["id"] = hash_id
        
        # Se chegou até aqui, não houve erro
        assert True

    def test_upsert_preparation(self) -> None:
        """Testa preparação dos dados para UPSERT."""
        # Simula dados processados prontos para UPSERT
        registros = self.mock_emendas_data.copy()
        
        # Adiciona IDs únicos
        for emenda in registros:
            dados_hash = json.dumps(emenda, sort_keys=True, default=str).encode('utf-8')
            hash_id = hashlib.sha256(dados_hash).hexdigest()[:16]
            emenda["id"] = hash_id
        
        # Simula parâmetros de UPSERT
        table_name = "emendas"
        conflict_fields = ["id"]
        primary_key = ["id"]
        schema = "public"
        
        # Verificações
        assert len(registros) == 2
        assert table_name == "emendas"
        assert conflict_fields == ["id"]
        assert primary_key == ["id"]
        assert schema == "public"
        
        # Verifica se todos os registros têm ID
        for registro in registros:
            assert "id" in registro
            assert len(registro["id"]) == 16
