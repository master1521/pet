"""
CreateTestData.py - Генератор тестовых данных для Apache Iceberg Data Lakehouse

Назначение:
    Модуль создает реалистичный набор тестовых данных для демонстрации работы
    с Apache Iceberg через PyIceberg.
    Генерирует 3 связанные таблицы в стиле e-commerce: пользователи, товары и заказы.

Архитектура данных:
    - users (измерение) → Справочник пользователей
    - products (измерение) → Каталог товаров
    - orders (факт) → Транзакционные данные заказов с FK на users и products

Таблицы:
    1. USERS (100,000 строк)
       ├─ user_id: LongType - уникальный ID пользователя
       ├─ name: StringType - имя (Alice, Bob, Charlie, Diana, Eve)
       ├─ email: StringType - email адрес
       ├─ age: LongType - возраст (18-70 лет)
       ├─ country: StringType - страна (US, UK, DE, FR, CA)
       ├─ is_active: BooleanType - активность
       └─ created_at: TimestampType - дата регистрации (2024)

    2. PRODUCTS (5,000 строк)
       ├─ product_id: LongType - уникальный ID товара
       ├─ product_name: StringType - название товара
       ├─ category: StringType - категория (Electronics, Clothing, Food, Books, Sports)
       ├─ brand: StringType - бренд товара
       ├─ price: DoubleType - цена ($10 - $2000)
       ├─ stock: LongType - количество на складе (0-500)
       ├─ rating: DoubleType - рейтинг (1.0-5.0)
       ├─ is_available: BooleanType - доступность
       └─ created_at: TimestampType - дата добавления (2023-2024)

    3. ORDERS (200,000 строк)
       ├─ order_id: LongType - уникальный ID заказа
       ├─ user_id: LongType - FK → users.user_id
       ├─ product_id: LongType - FK → products.product_id
       ├─ order_date: TimestampType - дата заказа (2024)
       ├─ quantity: LongType - количество товара (1-5)
       ├─ total_amount: DoubleType - общая сумма заказа
       ├─ status: StringType - статус (completed 85%, pending 10%, cancelled 4%, refunded 1%)
       └─ payment_method: StringType - способ оплаты (card, paypal, cash)

Функции:
    Схемы таблиц:
        - create_simple_users_schema() → Schema для таблицы users
        - create_products_schema() → Schema для таблицы products
        - create_orders_schema() → Schema для таблицы orders

    Генераторы данных:
        - generate_simple_users(n: int) → DataFrame с пользователями
        - generate_products(n: int) → DataFrame с товарами
        - generate_orders(n: int, n_users: int, n_products: int) → DataFrame с заказами

    Основная функция:
        - main() → Создает namespace 'sandbox', генерирует все таблицы, проверяет данные

Использование:
    python CreateTestData.py

    # Создает в Iceberg catalog:
    # - sandbox.users (100,000 строк)
    # - sandbox.products (5,000 строк)
    # - sandbox.orders (200,000 строк)

Особенности генерации:
    Users:
        - Равномерное распределение по странам
        - Все пользователи активны
        - Даты регистрации в течение 2024 года

    Products:
        - 5 категорий с разными брендами
        - Реалистичные ценовые диапазоны
        - 10% товаров недоступны (is_available=False)
        - Рейтинги от 1.0 до 5.0

    Orders:
        - 85% заказов завершены (completed)
        - Случайные связи user_id и product_id
        - Количество товаров: 1-5 штук
        - total_amount рассчитывается как quantity * random_price
        - 3 способа оплаты с равным распределением

"""


import logging
import pandas as pd
import random
from datetime import datetime, timedelta
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, StringType, BooleanType, TimestampType, DoubleType
)

from ExampleWorkWithIcebergPyiceberg import (
    connect_to_catalog,
    create_namespace,
    create_table,
    drop_table_if_exists,
    convert_dataframe_to_arrow,
    write_data_to_table,
    read_table_data_batched,
)

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.width', 2000)


# ==================== USERS ====================
def create_simple_users_schema() -> Schema:
    """Создать простую схему users - ВСЕ ПОЛЯ OPTIONAL."""
    return Schema(
        NestedField(1, "user_id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
        NestedField(3, "email", StringType(), required=False),
        NestedField(4, "age", LongType(), required=False),  # ✅ LongType
        NestedField(5, "country", StringType(), required=False),
        NestedField(6, "is_active", BooleanType(), required=False),
        NestedField(7, "created_at", TimestampType(), required=False)
    )


def generate_simple_users(n: int = 1000) -> pd.DataFrame:
    """Сгенерировать простые данные пользователей."""
    logger.info(f"🔄 Генерация {n:,} пользователей...")

    names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    countries = ["US", "UK", "DE", "FR", "CA"]

    df = pd.DataFrame({
        "user_id": range(1, n + 1),
        "name": [random.choice(names) for _ in range(n)],
        "email": [f"user{i}@example.com" for i in range(1, n + 1)],
        "age": [random.randint(18, 70) for _ in range(n)],
        "country": [random.choice(countries) for _ in range(n)],
        "is_active": [True for _ in range(n)],
        "created_at": [
            datetime(2024, 1, 1) + timedelta(days=random.randint(0, 330))
            for _ in range(n)
        ]
    })
    logger.info(f"✅ Сгенерировано {len(df):,} пользователей")
    return df


# ==================== PRODUCTS ====================
def create_products_schema() -> Schema:
    """Создать схему products (товары)."""
    return Schema(
        NestedField(1, "product_id", LongType(), required=False),
        NestedField(2, "product_name", StringType(), required=False),
        NestedField(3, "category", StringType(), required=False),
        NestedField(4, "brand", StringType(), required=False),
        NestedField(5, "price", DoubleType(), required=False),
        NestedField(6, "stock", LongType(), required=False),
        NestedField(7, "rating", DoubleType(), required=False),
        NestedField(8, "is_available", BooleanType(), required=False),
        NestedField(9, "created_at", TimestampType(), required=False)
    )


def generate_products(n: int = 1000) -> pd.DataFrame:
    """Сгенерировать данные товаров."""
    logger.info(f"🔄 Генерация {n:,} товаров...")

    categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
    brands = {
        "Electronics": ["Apple", "Samsung", "Sony", "LG"],
        "Clothing": ["Nike", "Adidas", "Zara", "H&M"],
        "Food": ["Nestle", "Coca-Cola", "PepsiCo"],
        "Books": ["Penguin", "Harper", "Random House"],
        "Sports": ["Wilson", "Spalding", "Decathlon"]
    }

    # Генерация категорий
    category_list = [random.choice(categories) for _ in range(n)]

    df = pd.DataFrame({
        "product_id": range(1, n + 1),
        "product_name": [f"Product_{cat[:3]}_{i}" for i, cat in enumerate(category_list, 1)],
        "category": category_list,
        "brand": [random.choice(brands[cat]) for cat in category_list],
        "price": [round(random.uniform(10.0, 2000.0), 2) for _ in range(n)],
        "stock": [random.randint(0, 500) for _ in range(n)],
        "rating": [round(random.uniform(1.0, 5.0), 1) for _ in range(n)],
        "is_available": [random.choice([True, False]) if i % 10 == 0 else True for i in range(n)],
        "created_at": [
            datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
            for _ in range(n)
        ]
    })
    logger.info(f"✅ Сгенерировано {len(df):,} товаров")
    logger.info(f"   Категории: {df['category'].value_counts().to_dict()}")
    return df


# ==================== ORDERS ====================
def create_orders_schema() -> Schema:
    """Создать схему orders (заказы)."""
    return Schema(
        NestedField(1, "order_id", LongType(), required=False),
        NestedField(2, "user_id", LongType(), required=False),
        NestedField(3, "product_id", LongType(), required=False),
        NestedField(4, "order_date", TimestampType(), required=False),
        NestedField(5, "quantity", LongType(), required=False),
        NestedField(6, "total_amount", DoubleType(), required=False),
        NestedField(7, "status", StringType(), required=False),
        NestedField(8, "payment_method", StringType(), required=False)
    )


def generate_orders(n: int = 10000, n_users: int = 100000, n_products: int = 1000) -> pd.DataFrame:
    """Сгенерировать данные заказов.
    Args:
        n: Количество заказов
        n_users: Количество пользователей (для FK)
        n_products: Количество товаров (для FK)
    Returns:
        DataFrame с заказами
    """
    logger.info(f"🔄 Генерация {n:,} заказов...")

    statuses = ["completed", "pending", "cancelled", "refunded"]
    status_weights = [0.85, 0.10, 0.04, 0.01]  # 85% completed

    payment_methods = ["card", "paypal", "cash"]

    # Генерация данных
    user_ids = [random.randint(1, n_users) for _ in range(n)]
    product_ids = [random.randint(1, n_products) for _ in range(n)]
    quantities = [random.randint(1, 5) for _ in range(n)]
    prices = [round(random.uniform(10.0, 2000.0), 2) for _ in range(n)]

    df = pd.DataFrame({
        "order_id": range(1, n + 1),
        "user_id": user_ids,
        "product_id": product_ids,
        "order_date": [
            datetime(2024, 1, 1) + timedelta(days=random.randint(0, 334))
            for _ in range(n)
        ],
        "quantity": quantities,
        "total_amount": [round(qty * price, 2) for qty, price in zip(quantities, prices)],
        "status": random.choices(statuses, weights=status_weights, k=n),
        "payment_method": [random.choice(payment_methods) for _ in range(n)]
    })

    logger.info(f"Сгенерировано {len(df):,} заказов")
    logger.info(f"Статусы: {df['status'].value_counts().to_dict()}")
    logger.info(f"   Общая сумма: ${df['total_amount'].sum():,.2f}")
    return df


def main():
    catalog = connect_to_catalog()
    namespace = "sandbox"
    create_namespace(catalog, namespace)

    #  ==================== USERS ====================
    drop_table_if_exists(catalog, f"{namespace}.users")
    schema_users = create_simple_users_schema()
    table_users = create_table(catalog, f"{namespace}.users", schema_users)
    users_df = generate_simple_users(n=100000)
    arrow_users = convert_dataframe_to_arrow(users_df)
    write_data_to_table(table_users, arrow_users)

    # ==================== PRODUCTS ====================
    drop_table_if_exists(catalog, f"{namespace}.products")
    schema_products = create_products_schema()
    table_products = create_table(catalog, f"{namespace}.products", schema_products)
    products_df = generate_products(n=5000)
    arrow_products = convert_dataframe_to_arrow(products_df)
    write_data_to_table(table_products, arrow_products)

    # ==================== ORDERS ====================
    drop_table_if_exists(catalog, f"{namespace}.orders")
    schema_orders = create_orders_schema()
    table_orders = create_table(catalog, f"{namespace}.orders", schema_orders)
    orders_df = generate_orders(n=200000, n_users=100000, n_products=5000)
    arrow_orders = convert_dataframe_to_arrow(orders_df)
    write_data_to_table(table_orders, arrow_orders)


                        # ПРОВЕРКА
    # USERS
    table_users = catalog.load_table(f"{namespace}.users")
    users_df = read_table_data_batched(table_users)
    print(users_df.head(10))
    print("#" * 100)
    # PRODUCTS
    table_products = catalog.load_table(f"{namespace}.products")
    products_df= read_table_data_batched(table_products)
    print(products_df.head(10))
    print("#" * 100)
    # ORDERS
    table_orders = catalog.load_table(f"{namespace}.orders")
    orders_df = read_table_data_batched(table_orders)
    print(orders_df.head(10))

if __name__ == "__main__":
    main()
