create table if not exists produto (
    Id_produto int not null,
    nome varchar(50),
    valor_unitario decimal(10),
    custo_unitario decimal(10),
    sub_categoria varchar(20),
    categoria varchar(20),
    sub_categoria_url varchar(100)
);