{{ config(materialized="table") }}

with
    emendas_raw as (
        select
            cast(codigoemenda as text) as codigo_emenda,
            cast(numeroemenda as text) as numero_emenda,
            cast(ano as int) as ano,
            cast(autor as text) as autor,
            cast(nomeautor as text) as nome_autor,
            cast(tipoemenda as text) as tipo_emenda,
            cast(localidadedogasto as text) as localidade_gasto,
            cast(funcao as text) as funcao,
            cast(subfuncao as text) as subfuncao,
            
            case
                when valorempenhado is null or trim(cast(valorempenhado as text)) = ''
                then 0.0
                else
                    cast(
                        replace(
                            replace(cast(valorempenhado as text), '.', ''), ',', '.'
                        ) as numeric(15, 2)
                    )
            end as valor_empenhado,
            
            case
                when valorliquidado is null or trim(cast(valorliquidado as text)) = ''
                then 0.0
                else
                    cast(
                        replace(
                            replace(cast(valorliquidado as text), '.', ''), ',', '.'
                        ) as numeric(15, 2)
                    )
            end as valor_liquidado,
            
            case
                when valorpago is null or trim(cast(valorpago as text)) = ''
                then 0.0
                else
                    cast(
                        replace(
                            replace(cast(valorpago as text), '.', ''), ',', '.'
                        ) as numeric(15, 2)
                    )
            end as valor_pago,
            
            current_timestamp as data_carga

        from {{ source("portal_transparencia", "emendas") }}
    )

select *
from emendas_raw
