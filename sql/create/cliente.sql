create table if not exists cliente (
    id_cliente int not null,
    nome varchar(10),
    tipo varchar(30),
    media_de_alunos int,
    classificacao char(1),
    id_cidade int not null
);