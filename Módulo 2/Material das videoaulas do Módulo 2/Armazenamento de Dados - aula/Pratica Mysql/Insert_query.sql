/* Logico_cliente: */

CREATE TABLE teste.Cliente (
    cod_cliente INT PRIMARY KEY,
    nom_cliente VARCHAR(30)
);

CREATE TABLE teste.Endereco (
    id_endereco INT PRIMARY KEY,
    nom_endereco VARCHAR(10) UNIQUE,
    dsc_endereco VARCHAR(60),
    fk_Cliente_cod_cliente INTEGER
);
 
ALTER TABLE teste.Endereco ADD CONSTRAINT FK_Endereco_2
    FOREIGN KEY (fk_Cliente_cod_cliente)
    REFERENCES teste.Cliente (cod_cliente)
    ON DELETE RESTRICT;




INSERT INTO teste.cliente(cod_cliente, nom_cliente) VALUES
(1, 'Thaddeus'),
(2, 'Alisa'),
(3, 'Leroy'),
(4, 'Melvin'),
(5, 'Marshall'),
(6, 'Winifred'),
(7, 'Ira'),
(8, 'Amal'),
(9, 'Lila'),
(10, 'Damon');

INSERT INTO teste.endereco(id_endereco, nom_endereco, dsc_endereco, fk_Cliente_cod_cliente) VALUES
(1, 'Endereco 1', 'Rua xxx1, 100 Bairro yyy', 1),
(2, 'Endereco 2', 'Rua xxx2, 100 Bairro yyy', 2),
(3, 'Endereco 3', 'Rua xxx3, 100 Bairro yyy', 3),
(4, 'Endereco 4', 'Rua xxx4, 100 Bairro yyy', 4),
(5, 'Endereco 5', 'Rua xxx5, 100 Bairro yyy', 5);

select * 
from teste.cliente 
inner join teste.endereco on cliente.cod_cliente = endereco.fk_Cliente_cod_cliente