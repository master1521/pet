import datetime
import psycopg2
import x_config
from random import randint
import logging

from faker import Faker
fake = Faker(['ru_RU'])

logger = logging.getLogger()
logging.basicConfig(level='INFO')

try:
    # Подключение к БД
    connection = psycopg2.connect(dbname=x_config.dbname, user=x_config.user, password=x_config.password,
                                  host=x_config.host, port=x_config.port)
    print(f'[INFO] Соединение с базой открыто')

    connection.autocommit = True

    # Добавляем данные в таблицы
    with connection.cursor() as cursor:

        for date in range(1500):
            random_date = fake.date_time_between(start_date=datetime.date(2015, 1, 1), end_date=datetime.date(2022, 1, 1))

            cursor.execute(
                f""" INSERT INTO main.date (dt)
                VALUES
                    ('{random_date}') """)
            logger.info(f'Добавлена дата {random_date}')

        cursor.execute(
            f"""INSERT INTO main.gender (gender)
            VALUES
                ('Male')
                ,('Female') """)
        logger.info(f'Добавлен пол Male amd Female')

        for i in range(50):
            cursor.execute(
                f"""INSERT INTO "main".publisher ( publisher )
                VALUES
                    ( '{fake.company()}')""")
            logger.info(f'Добавлен издатель {fake.company()}')

        for i in range(1000):
            cursor.execute(
                f"""INSERT INTO "main".country (country)
                   VALUES
                       ('{fake.country()}')""")
            logger.info(f'Добавлена страна {fake.country()}')

        for i in range(1000):
            cursor.execute(
                f"""INSERT INTO "main".city ( id_country, city)
                VALUES
                    ( '{randint(1, 1000)}', '{fake.city()}')""")
            logger.info(f'Добавлен город {fake.city()}')

        for i in range(1000):
            cursor.execute(
                f"""INSERT INTO "main".address ( id_city, address)
                VALUES
                    ( '{randint(1, 1000)}', '{fake.address()}')""")
            logger.info(f'Добавлен адрес {fake.address()}')

        gener = "Action Animation Children Classics Comedy Documentary Drama Family Foreign Games Horror Music New Sci-Fi Sports Travel Anime Family Animal Detectiv NonFiction Fiction Siens".split()
        for i in gener:
            cursor.execute(
                f"""INSERT INTO main.gener (gener)
                VALUES
                    ( '{i}')""")

        for i in range(1000):
            cursor.execute(
                f"""INSERT INTO main.autor (first_name, last_name, id_gender)
                VALUES
                    ('{fake.first_name()}', '{fake.last_name()}', '{randint(1,2)}')""")

        for i in range(1500):
            cursor.execute(
                f"""INSERT INTO "main".book (title, price, pages, release_year, id_publisher)
                VALUES
                    ('{fake.word().title()}', '{randint(100, 1500)}',
                     '{randint(80, 500)}', '{fake.date(pattern="%Y-%m-%d")}', {randint(1, 50)} )""")

        for i in range(1000):
            cursor.execute(
                f"""INSERT INTO "main".customer ( first_name, last_name, id_address, phone, email, id_gender)
                VALUES
                    ('{fake.first_name()}', '{fake.last_name()}', {randint(1, 1000)}, '{fake.phone_number()}', '{fake.email()}', {randint(1,2)} )""")

        for i in range(10):
            cursor.execute(
                f"""INSERT INTO "main".shop ( id_address, name, phone )
                VALUES
                    ({randint(1,1000)}, '{fake.word().title()}', '{fake.phone_number()}')""")

        for i in range(500):
            try:
                cursor.execute(
                    f"""INSERT INTO "main".book_autor ( id_book, id_autor )
                    VALUES
                        ( '{randint(1,1500)}', '{randint(1, 1000)}')""")
            except:
                pass

        for i in range(500):
            try:
                cursor.execute(
                    f"""INSERT INTO "main".book_gener ( id_book, id_gener )
                    VALUES
                        ( '{randint(1,1500)}', '{randint(1, 16)}')
                    """)
            except:
                pass

        for i in range(2000):
            try:
                cursor.execute(
                    f"""INSERT INTO "main".sales ( id_shop, id_product, id_customer, id_date, quantity )
                    VALUES
                        ( {randint(1,10)}, {randint(1, 1500)}, {randint(1,1000)}, {randint(1,1500)}, {randint(1, 10)})
                    """)
            except:
                pass

except Exception as _ex:
    print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if connection:
        connection.close()
        print("[INFO] Соединение с базой закрыто")

