{{ config(materialized="table") }}

with
    source as (select * from {{ source("camara_deputados", "deputados") }}),

    renamed as (
        select
            cast(nullif(trim(id), '') as integer) as id_deputado,
            nullif(trim(uri), '') as uri_deputado,
            nullif(trim(nome), '') as nome_deputado,
            nullif(trim(siglapartido), '') as sigla_partido,
            nullif(trim(uripartido), '') as uri_partido,
            upper(nullif(trim(siglauf), '')) as sigla_uf,
            cast(nullif(trim(idlegislatura), '') as integer) as id_legislatura,
            nullif(trim(urlfoto), '') as url_foto,
            nullif(trim(email), '') as email_deputado,
            cast(dt_ingest as timestamp) as data_ingestao
        from source
    )

select *
from renamed
