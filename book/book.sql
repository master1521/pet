-- CREATE SCHEMA dim


-- Создаем таблицы
CREATE TABLE "dim".gender (
	id_gender serial PRIMARY KEY
	,gender varchar(10) 
)

CREATE TABLE "dim".country (
	id_country serial PRIMARY KEY
	,country varchar(50) NOT NULL
)

CREATE TABLE "dim".city (
	id_city serial PRIMARY KEY
	,id_country integer NOT NULL REFERENCES book."dim".country (id_country)
	,city varchar(50) NOT NULL
)

CREATE TABLE "dim".address (
	id_address serial PRIMARY KEY
	,id_city integer NOT NULL REFERENCES book."dim".city (id_city)
	,address varchar(200) NOT NULL
)

CREATE TABLE "dim".autor (
	id_autor serial PRIMARY KEY
	,id_gender INTEGER NOT NULL REFERENCES book."dim".gender (id_gender)
	,id_country INTEGER NOT NULL REFERENCES book."dim".country (id_country)
    ,first_name varchar(100) NOT NULL
    ,last_name varchar(100) NOT NULL
)

CREATE TABLE "dim".customer (
	id_customer serial PRIMARY KEY
	,id_address integer NOT NULL REFERENCES book."dim".address (id_address)
	,first_name varchar(50) NOT NULL
	,last_name varchar(50) NOT NULL
)

CREATE TABLE "dim".phone (
	id_phone serial PRIMARY KEY 
	,id_customer integer NOT NULL REFERENCES book."dim".customer (id_customer)
	,phone varchar(50) NOT NULL
)

CREATE TABLE "dim".publisher (
	id_publisher serial PRIMARY KEY
	,publisher varchar(100) NOT NULL
)

CREATE TABLE "dim".gener (
	id_gener serial PRIMARY KEY
	,gener varchar(50) NOT NULL
)

CREATE TABLE "dim".book (
	id_book serial PRIMARY KEY
	,id_autor integer NOT NULL REFERENCES book."dim".autor (id_autor)
	,id_gener integer NOT NULL REFERENCES book."dim".gener (id_gener)
	,id_publisher integer NOT NULL REFERENCES book."dim".publisher (id_publisher)
	,title varchar(100) NOT NULL
	,pages integer NOT NULL
	,release_year date NOT NULL CHECK (release_year < now()::date)
)

CREATE TABLE "dim".book_autor (
	id_book integer NOT NULL 
	,id_autor integer NOT NULL
	,PRIMARY KEY (id_book, id_autor)
	,FOREIGN KEY (id_book) REFERENCES book."dim".book (id_book)
	,FOREIGN KEY (id_autor) REFERENCES book."dim".autor (id_autor)
)

CREATE TABLE "dim".book_gener (
	id_book integer NOT NULL
	,id_gener integer NOT NULL
	,PRIMARY KEY (id_book, id_gener)
	,FOREIGN KEY (id_book) REFERENCES book."dim".book (id_book)
	,FOREIGN KEY (id_gener) REFERENCES book."dim".gener (id_gener)
)

CREATE TABLE "dim".payment (
	id_payment serial PRIMARY KEY
	,id_customer integer NOT NULL REFERENCES book."dim".customer (id_customer)
	,id_book integer NOT NULL REFERENCES book."dim".book (id_book)
	,payment_date date NOT NULL
)





































	
	
