with
    fonte_territorio as (
        select
           cast(territory_id as int) as id_territorio_funcionario
           , cast(territory_description as string) as descricao_territorio
           , cast(region_id as int) as id_regiao
        from {{ source('erp', 'territories') }}    
    )
select *
from fonte_territorio