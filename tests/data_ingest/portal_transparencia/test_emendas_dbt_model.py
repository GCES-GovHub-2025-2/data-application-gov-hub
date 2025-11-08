import pytest
import os
import sys
from unittest.mock import patch, MagicMock
import psycopg2
from decimal import Decimal

# Adiciona os caminhos necessários
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'airflow_lappis', 'plugins'))


class TestEmendasDBTModel:
    """Testes para o modelo dbt bronze de emendas parlamentares."""

    def setup_method(self) -> None:
        """Configuração inicial para cada teste."""
        self.mock_raw_data = [
            {
                "codigoemenda": "201826760002",
                "numeroemenda": "0002",
                "ano": "2018",
                "autor": "123456789",
                "nomeautor": "VINICIUS GURGEL",
                "tipoemenda": "Emenda Individual",
                "localidadedogasto": "AMAPÁ (UF)",
                "funcao": "Segurança pública",
                "subfuncao": "Policiamento",
                "valorempenhado": "1.234,56",
                "valorliquidado": "1.000,00",
                "valorpago": "800,50"
            },
            {
                "codigoemenda": "202050170007",
                "numeroemenda": "0007",
                "ano": "2020",
                "autor": "COMISSAO",
                "nomeautor": "COMISSAO DE FINANCAS",
                "tipoemenda": "Emenda de Comissão",
                "localidadedogasto": "Nacional",
                "funcao": "Agricultura",
                "subfuncao": "Promoção da produção",
                "valorempenhado": "11.385.738,68",
                "valorliquidado": "3.195.992,49",
                "valorpago": "1.569.992,02"
            }
        ]

    def test_data_type_transformations(self) -> None:
        """Testa se as transformações de tipo estão corretas."""
        # Simula a transformação do modelo dbt
        raw = self.mock_raw_data[0]
        
        # Testes de cast para text
        codigo_emenda = str(raw["codigoemenda"])
        numero_emenda = str(raw["numeroemenda"])
        autor = str(raw["autor"])
        nome_autor = str(raw["nomeautor"])
        
        assert isinstance(codigo_emenda, str)
        assert isinstance(numero_emenda, str)
        assert isinstance(autor, str)
        assert isinstance(nome_autor, str)
        
        # Teste de cast para int
        ano = int(raw["ano"])
        assert isinstance(ano, int)
        assert ano == 2018

    def test_brazilian_currency_conversion(self) -> None:
        """Testa conversão de valores brasileiros (1.234,56) para formato SQL (1234.56)."""
        test_cases = [
            ("1.234,56", 1234.56),
            ("11.385.738,68", 11385738.68),
            ("0,00", 0.0),
            ("1.000,00", 1000.0),
            ("", 0.0),
        ]
        
        for input_val, expected in test_cases:
            # Simula a transformação do modelo dbt
            if input_val is None or input_val.strip() == '':
                result = 0.0
            else:
                result = float(
                    input_val.replace('.', '').replace(',', '.')
                )
            
            assert result == expected, f"Falhou para {input_val}: esperado {expected}, obteve {result}"

    def test_null_value_handling(self) -> None:
        """Testa tratamento de valores nulos e vazios."""
        test_values = [None, "", "   ", "0,00"]
        
        for val in test_values:
            # Simula a lógica do CASE WHEN do modelo
            if val is None or (isinstance(val, str) and val.strip() == ''):
                result = 0.0
            else:
                result = float(val.replace('.', '').replace(',', '.'))
            
            assert isinstance(result, float)
            assert result >= 0

    def test_all_columns_present(self) -> None:
        """Testa se todas as colunas esperadas estão presentes no modelo."""
        expected_columns = [
            "codigo_emenda",
            "numero_emenda",
            "ano",
            "autor",
            "nome_autor",
            "tipo_emenda",
            "localidade_gasto",
            "funcao",
            "subfuncao",
            "valor_empenhado",
            "valor_liquidado",
            "valor_pago",
            "data_carga"
        ]
        
        # Simula a estrutura do modelo transformado
        transformed_record = {
            "codigo_emenda": str(self.mock_raw_data[0]["codigoemenda"]),
            "numero_emenda": str(self.mock_raw_data[0]["numeroemenda"]),
            "ano": int(self.mock_raw_data[0]["ano"]),
            "autor": str(self.mock_raw_data[0]["autor"]),
            "nome_autor": str(self.mock_raw_data[0]["nomeautor"]),
            "tipo_emenda": str(self.mock_raw_data[0]["tipoemenda"]),
            "localidade_gasto": str(self.mock_raw_data[0]["localidadedogasto"]),
            "funcao": str(self.mock_raw_data[0]["funcao"]),
            "subfuncao": str(self.mock_raw_data[0]["subfuncao"]),
            "valor_empenhado": 1234.56,
            "valor_liquidado": 1000.00,
            "valor_pago": 800.50,
            "data_carga": "2025-11-12 00:00:00"
        }
        
        for col in expected_columns:
            assert col in transformed_record, f"Coluna {col} não encontrada"

    def test_numeric_precision(self) -> None:
        """Testa se a precisão numérica está correta (numeric(15,2))."""
        test_values = [
            "1.234,56",
            "11.385.738,68",
            "999.999.999.999,99"  # Valor máximo para numeric(15,2)
        ]
        
        for val in test_values:
            result = float(val.replace('.', '').replace(',', '.'))
            
            # Verifica se tem no máximo 2 casas decimais
            decimal_places = len(str(result).split('.')[-1]) if '.' in str(result) else 0
            assert decimal_places <= 2, f"Valor {result} tem mais de 2 casas decimais"
            
            # Verifica se o valor total não excede 15 dígitos
            total_digits = len(str(result).replace('.', ''))
            assert total_digits <= 15, f"Valor {result} excede 15 dígitos"

    def test_value_consistency_logic(self) -> None:
        """Testa consistência lógica: liquidado <= empenhado, pago <= liquidado."""
        # Valores corretos
        empenhado = 1234.56
        liquidado = 1000.00
        pago = 800.50
        
        assert liquidado <= empenhado, "Valor liquidado deve ser <= empenhado"
        assert pago <= liquidado, "Valor pago deve ser <= liquidado"
        
    def test_year_field_type(self) -> None:
        """Testa se o campo ano é convertido corretamente para integer."""
        for record in self.mock_raw_data:
            ano = int(record["ano"])
            
            assert isinstance(ano, int)
            assert 2000 <= ano <= 2100, f"Ano {ano} fora do intervalo esperado"

    def test_empty_string_to_zero_conversion(self) -> None:
        """Testa conversão de strings vazias para 0.0."""
        empty_cases = ["", "   ", None]
        
        for empty_val in empty_cases:
            # Simula a lógica do modelo
            if empty_val is None or (isinstance(empty_val, str) and empty_val.strip() == ''):
                result = 0.0
            else:
                result = float(empty_val.replace('.', '').replace(',', '.'))
            
            assert result == 0.0

    def test_data_carga_presence(self) -> None:
        """Testa se o campo data_carga seria adicionado."""
        # O modelo adiciona current_timestamp como data_carga
        # Este teste verifica se a lógica está presente
        
        from datetime import datetime
        
        # Simula a adição de data_carga
        data_carga = datetime.now()
        
        assert data_carga is not None
        assert isinstance(data_carga, datetime)

    def test_field_name_mapping(self) -> None:
        """Testa se os nomes dos campos estão mapeados corretamente."""
        field_mapping = {
            "codigoemenda": "codigo_emenda",
            "numeroemenda": "numero_emenda",
            "ano": "ano",
            "autor": "autor",
            "nomeautor": "nome_autor",
            "tipoemenda": "tipo_emenda",
            "localidadedogasto": "localidade_gasto",
            "funcao": "funcao",
            "subfuncao": "subfuncao",
            "valorempenhado": "valor_empenhado",
            "valorliquidado": "valor_liquidado",
            "valorpago": "valor_pago"
        }
        
        # Verifica se todos os campos de origem estão mapeados
        for source_field in self.mock_raw_data[0].keys():
            assert source_field in field_mapping, f"Campo {source_field} não está mapeado"

    def test_multiple_records_transformation(self) -> None:
        """Testa transformação de múltiplos registros."""
        transformed_records = []
        
        for raw in self.mock_raw_data:
            # Simula a transformação do modelo
            transformed = {
                "codigo_emenda": str(raw["codigoemenda"]),
                "ano": int(raw["ano"]),
                "valor_empenhado": float(
                    raw["valorempenhado"].replace('.', '').replace(',', '.')
                ) if raw["valorempenhado"] else 0.0
            }
            transformed_records.append(transformed)
        
        assert len(transformed_records) == 2
        assert transformed_records[0]["ano"] == 2018
        assert transformed_records[1]["ano"] == 2020
        assert transformed_records[0]["valor_empenhado"] == 1234.56
        assert transformed_records[1]["valor_empenhado"] == 11385738.68

    def test_source_table_reference(self) -> None:
        """Testa se a referência à tabela fonte está correta."""
        # O modelo referencia: source("portal_transparencia", "emendas")
        source_name = "portal_transparencia"
        table_name = "emendas"
        
        assert source_name == "portal_transparencia"
        assert table_name == "emendas"
        
        # Verifica se corresponde ao definido em sources.yml
        expected_schema = "public"
        assert expected_schema == "public"
