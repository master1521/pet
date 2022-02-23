import psycopg2
import x_config
from random import randint

from faker import Faker
fake = Faker(['ru_RU'])


try:
    # СОЗДАТЬ ПОДКЛЮЧЕНИЕ К БД
    connection = psycopg2.connect(dbname=x_config.dbname, user=x_config.user, password=x_config.password,
                                  host=x_config.host, port=x_config.port)
    print(f'[INFO] Соединение с базой открыто')

    # АВТО КОММИТ
    connection.autocommit = True

    # # СОЗДАТЬ ТАБЛИЦЫ
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".gender (
    #             id_gender serial PRIMARY KEY
    #             ,gender varchar(10)) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".country (
    #             id_country serial PRIMARY KEY
    #             ,country varchar(50) NOT NULL) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".city (
    #             id_city serial PRIMARY KEY
    #             ,id_country integer NOT NULL REFERENCES book."dim".country (id_country)
    #             ,city varchar(50) NOT NULL) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".address (
    #             id_address serial PRIMARY KEY
    #             ,id_city integer NOT NULL REFERENCES book."dim".city (id_city)
    #             ,address varchar(200) NOT NULL) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".autor (
    #             id_autor serial PRIMARY KEY
    #             ,id_gender INTEGER NOT NULL REFERENCES book."dim".gender (id_gender)
    #             ,id_country INTEGER NOT NULL REFERENCES book."dim".country (id_country)
    #             ,first_name varchar(100) NOT NULL
    #             ,last_name varchar(100) NOT NULL) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".customer (
    #             id_customer serial PRIMARY KEY
    #             ,id_address integer NOT NULL REFERENCES book."dim".address (id_address)
    #             ,first_name varchar(50) NOT NULL
    #             ,last_name varchar(50) NOT NULL) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".phone (
    #             id_phone serial PRIMARY KEY
    #             ,id_customer integer NOT NULL REFERENCES book."dim".customer (id_customer)
    #             ,phone varchar(50) NOT NULL) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".publisher (
    #             id_publisher serial PRIMARY KEY
    #             ,publisher varchar(100) NOT NULL) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".gener (
    #             id_gener serial PRIMARY KEY
    #             ,gener varchar(50) NOT NULL) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".book (
    #             id_book serial PRIMARY KEY
    #             ,id_publisher integer NOT NULL REFERENCES book."dim".publisher (id_publisher)
    #             ,title varchar(100) NOT NULL
    #             ,pages integer NOT NULL
    #             ,price integer NOT NULL
    #             ,release_year date NOT NULL CHECK (release_year < now()::date)) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".book_autor (
    #             id_book integer NOT NULL
    #             ,id_autor integer NOT NULL
    #             ,PRIMARY KEY (id_book, id_autor)
    #             ,FOREIGN KEY (id_book) REFERENCES book."dim".book (id_book)
    #             ,FOREIGN KEY (id_autor) REFERENCES book."dim".autor (id_autor)) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")
    #
    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """CREATE TABLE "dim".book_gener (
    #             id_book integer NOT NULL
    #             ,id_gener integer NOT NULL
    #             ,PRIMARY KEY (id_book, id_gener)
    #             ,FOREIGN KEY (id_book) REFERENCES book."dim".book (id_book)
    #             ,FOREIGN KEY (id_gener) REFERENCES book."dim".gener (id_gener)) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")

    # with connection.cursor() as cursor:
    #         cursor.execute(
    #             """ CREATE TABLE "dim".payment (
    #             id_payment serial PRIMARY KEY
    #             ,id_customer integer NOT NULL REFERENCES book."dim".customer (id_customer)
    #             ,id_book integer NOT NULL REFERENCES book."dim".book (id_book)
    #             ,payment_date date NOT NULL) """
    #         )
    #         print("[INFO] Таблица автор успешно создана")

    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ ПОЛ
    # with connection.cursor() as cursor:
    #     cursor.execute(
    #         f"""INSERT INTO "dim".gender (gender)
    #         VALUES
    #             ('Male')
    #             ,('Female')
    #         """)
    # print("[INFO] Данные были успешно добавлены в таблицу пол")
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ СТРАН
    # with connection.cursor() as cursor:
    #     for i in range(1000):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".country (country)
    #                VALUES
    #                    ('{fake.country()}')
    #                """)
    # print("[INFO] Данные были успешно добавлены в таблицу страна")
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ АВТОР
    # with connection.cursor() as cursor:
    #     for i in range(2000):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".autor ( id_gender, id_country, first_name, last_name)
    #             VALUES
    #                 ( '{randint(1,2)}', '{randint(1,1000)}', '{fake.first_name()}', '{fake.last_name()}')
    #             """)
    # print("[INFO] Данные были успешно добавлены в таблицу автор")
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ ГОРОД
    # with connection.cursor() as cursor:
    #     for i in range(1000):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".city ( id_country, city)
    #             VALUES
    #                 ( '{randint(1, 1000)}', '{fake.city()}')
    #             """)
    # print("[INFO] Данные были успешно добавлены в таблицу город")
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ АДРЕС
    # with connection.cursor() as cursor:
    #     for i in range(1000):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".address ( id_city, address)
    #             VALUES
    #                 ( '{randint(1, 1000)}', '{fake.address()}')
    #             """)
    # print("[INFO] Данные были успешно добавлены  таблицу адрес")
    #
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ ПОКУПАТЕЛЬ
    # with connection.cursor() as cursor:
    #     for i in range(1000):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".customer ( id_address, first_name, last_name)
    #             VALUES
    #                 ( '{randint(1, 1000)}', '{fake.first_name()}', '{fake.last_name()}')
    #             """)
    # print("[INFO] Данные были успешно добавлены  таблицу покупатель")
    #
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ ТЕЛЕФОН
    # with connection.cursor() as cursor:
    #     for i in range(1500):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".phone ( id_customer, phone)
    #             VALUES
    #                 ( '{randint(1, 1000)}', '{fake.phone_number()}')
    #             """)
    # print("[INFO] Данные были успешно добавлены таблицу телефон")
    #
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ ИЗДАТЕЛЬ
    # with connection.cursor() as cursor:
    #     for i in range(50):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".publisher ( publisher )
    #             VALUES
    #                 ( '{fake.company()}')
    #             """)
    # print("[INFO] Данные были успешно добавлены таблицу издатель")
    #
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ ЖАНР
    # with connection.cursor() as cursor:
    #     gener = "Action Animation Children Classics Comedy Documentary Drama Family Foreign Games Horror Music New Sci-Fi Sports Travel Anime Family Animal Detectiv NonFiction Fiction Siens".split()
    #
    #     for i in gener:
    #         cursor.execute(
    #             f"""INSERT INTO "dim".gener ( gener )
    #             VALUES
    #                 ( '{i}')
    #             """)
    # print("[INFO] Данные были успешно добавлены таблицу жанр")
    #
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ КНИГИ
    # with connection.cursor() as cursor:
    #     for i in range(3000):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".book ( id_publisher, title, pages, price, release_year)
    #             VALUES
    #                 ( '{randint(1, 50)}', '{fake.word().title()}', '{randint(80, 500)}', '{randint(100, 1500)}', '{fake.date(pattern="%Y-%m-%d")}')
    #             """)
    # print("[INFO] Данные были успешно добавлены  таблицу книги")
    #
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ КНИГИ_АВТОР
    # # Выдает ошибку из-за ключей, нужно несколько раз запустить чтобы данных было больше
    # with connection.cursor() as cursor:
    #     for i in range(1000):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".book_autor ( id_book, id_autor )
    #             VALUES
    #                 ( '{randint(1,2000)}', '{randint(1, 1000)}')
    #             """)
    # print("[INFO] Данные были успешно добавлены таблицу книги_автор")
    #
    # # ВСТАВИТЬ ДАННЫЕ В ТАБЛИЦУ КНИГИ_ЖАНР
    # # Выдает ошибку из-за ключей, нужно несколько раз запустить чтобы данных было больше (3)
    # with connection.cursor() as cursor:
    #     for i in range(500):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".book_gener ( id_book, id_gener )
    #             VALUES
    #                 ( '{randint(1,3000)}', '{randint(1, 16)}')
    #             """)
    # print("[INFO] Данные были успешно добавлены таблицу книги_жанр")
    
    # ВСТАВИТЬ ДАННЫЕ В ТААЛИЦУ ПЛАТЕЖИ
    # with connection.cursor() as cursor:
    #     for i in range(1000):
    #         cursor.execute(
    #             f"""INSERT INTO "dim".payment ( id_customer, id_book, payment_date )
    #             VALUES
    #                 ( '{randint(1,1000)}', '{randint(1, 3000)}', '{fake.date(pattern="%Y-%m-%d")}')
    #             """)
    # print("[INFO] Данные были успешно добавлены таблицу платежи")
    
except Exception as _ex:
    print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if connection:
        connection.close()
        print("[INFO] Соединение с базой закрыто")






















