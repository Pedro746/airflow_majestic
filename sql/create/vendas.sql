create table if not exists vendas (
    Id_venda int not null,
    id_nota int not null,
    data date,
    frente int,
    desconto int,
    vendedor int,
    cliente int,
    produto int,
    quantidade int,
    filial int,
    forma_de_pagamento int,
    data_de_faturamento int

);