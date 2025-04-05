{{ config(materialized="view") }}

with

    nc_transfere_gov as (
        select distinct id_plano_acao, tx_numero_nota as nc, ndc.cd_ug_emitente_nota as ug
        from {{ source("transfere_gov", "notas_de_credito") }} ndc
        where ndc.tx_numero_nota is not null
    ),

    nc_siafi as (
        select distinct
            left(nc, 6) as ug, right(nc, 12) as nc, nt.nc_transferencia as num_transf
        from {{ source("siafi", "ncs_tesouro") }} nt
        where nc_transferencia != '-8'
    ),

    result_table as (
        select distinct num_transf, id_plano_acao as plano_acao
        from nc_siafi
        left join nc_transfere_gov using (nc, ug)
    )

--
select *
from result_table
