
{{ config(materialized="table") }}

with source as (
    select * from {{ source('camara_deputados', 'deputados') }}
),

renamed as (
    select
        cast(id as integer) as id_deputado,
        uri as uri_deputado,
        nome as nome_deputado,
        siglapartido as sigla_partido,
        uripartido as uri_partido,
        siglauf as sigla_uf,
        cast(idlegislatura as integer) as id_legislatura,
        urlfoto as url_foto,
        email as email_deputado,
        cast(dt_ingest as timestamp) as data_ingestao
    from source
)

select * from renamed
