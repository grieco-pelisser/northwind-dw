with
    stg_transportadoras as (
        select *
        from {{ ref('stg_erp__transportadoras') }}
    )

    , criar_chave as (
        select
            row_number() over(order by id_transportadora) as pk_transportadora
            , *
        from stg_transportadoras
    )

select *
from criar_chave