with
    stg_funcionarios as(
       select *
       from {{ ref('stg_erp__funcionarios') }}
    )

    , transformacoes as(
        select 
            id_funcionario
            , sobrenome_funcionario
            , nome_funcionario
            , funcao_funcionario
            , titulo_funcionario
            , data_nascimento_funcionario
            , data_contratacao_funcionario
            , endereco_funcionario
            , cidade_funcionario
            , cep_funcionario
            , pais_funcionario
        from stg_funcionarios
       )

    , criar_chave as (
            select
                row_number() over(order by id_funcionario) as pk_funcionario
                , concat(nome_funcionario, ' ' , sobrenome_funcionario) as nome_completo_funcionario
                , *
           from transformacoes
       )

select *
from criar_chave