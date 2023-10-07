with
    dim_produtos as(
        select*
        from {{ ref('dim_produtos') }}
    )

    , dim_clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , pedido_itens as (
        select *
        from {{ ref('int_vendas__pedido_itens') }}
    )

    , dim_transportadoras as (
        select *
        from {{ ref('dim_transportadoras') }}
    )

    , dim_funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , join_tabelas as (
        select
            pedidos.sk_pedido_item
            , pedidos.id_pedido
            , pedidos.id_funcionario
            , dim_clientes.pk_cliente
            , pedidos.id_transportadora
            , dim_produtos.pk_produto
            , dim_funcionarios.pk_funcionario
            , pedidos.data_do_pedido
            , pedidos.frete
            , pedidos.destinatario
            , pedidos.endereco_destinatario
            , pedidos.cep_destinatario
            , pedidos.cidade_destinatario
            , pedidos.regiao_destinatario
            , pedidos.pais_destinatario
            , pedidos.data_do_envio
            , pedidos.data_requerida_entrega
            , pedidos.desconto_perc
            , pedidos.preco_da_unidade
            , pedidos.quantidade
            , dim_produtos.nome_produto
            , dim_produtos.nome_categoria
            , dim_produtos.is_discontinuado
            , dim_produtos.nome_fornecedor
            , dim_clientes.nome_cliente
            , dim_funcionarios.nome_completo_funcionario
            , dim_funcionarios.funcao_funcionario
            , dim_funcionarios.cidade_funcionario
            , dim_funcionarios.pais_funcionario
            , dim_transportadoras.nome_transportadora
        from pedido_itens as pedidos
            left join dim_produtos on
                pedidos.id_produto = dim_produtos.id_produto
            left join dim_clientes on 
                pedidos.id_cliente = dim_clientes.id_cliente
            left join dim_funcionarios on
                pedidos.id_funcionario = dim_funcionarios.id_funcionario
            left join dim_transportadoras on
                pedidos.id_transportadora = dim_transportadoras.id_transportadora
    )

    , transformacoes as (
        select
            *
            , preco_da_unidade * quantidade as total_bruto
            , (1 - desconto_perc) * preco_da_unidade * quantidade as total_liquido
            , case 
                when desconto_perc > 0 then true
                when desconto_perc = 0 then false
                else false
            end as is_desconto
            , frete / count(id_pedido) over(partition by id_pedido) as frete_ponderado
        from join_tabelas
    )

select *
from transformacoes
