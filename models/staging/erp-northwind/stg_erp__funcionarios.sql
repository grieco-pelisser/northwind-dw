with
    fonte_funcionarios as (
        select
            cast(employee_id as int) as id_funcionario
            , cast(last_name as string) as sobrenome_funcionario
            , cast(first_name as string) as nome_funcionario
            , cast(title as string) as funcao_funcionario
            , cast(title_of_courtesy as string) as titulo_funcionario
            , cast(birth_date as date) as data_nascimento_funcionario
            , cast(hire_date as date) as data_contratacao_funcionario
            , cast(address as string) as endereco_funcionario
            , cast(city as string) as cidade_funcionario
            --, region 
            , cast(postal_code as string) as cep_funcionario
            , cast(country as string) as pais_funcionario
            --,home_phone  
            --, extension
            --, photo 
            --, notes 
            --, reports_to 
            --, photo_path 
        from {{ source('erp', 'employees') }}
    )
select *
from fonte_funcionarios