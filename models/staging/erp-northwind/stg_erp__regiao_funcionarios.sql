with
    fonte_regiao_funcionarios as (
        select
            cast(region_id as int) as id_regiao
            , cast(region_description as string) as descricao_regiao
        from {{ source('erp', 'region') }}    
    )
select *
from fonte_regiao_funcionarios