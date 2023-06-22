create table if not exists localizacao (
    id_cidade int not null,
    cidade varchar(20),
    estado varchar(20),
    pais varchar(20),
    continente varchar(20),
    bandeira_url varchar(100)

);