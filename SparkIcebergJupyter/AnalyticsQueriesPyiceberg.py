"""
Аналитические запросы для тестового Data Lakehouse.
"""

import logging
import pandas as pd
from pyiceberg.expressions import (
    GreaterThan, LessThan, EqualTo, And, Or, In
)

from ExampleWorkWithIcebergPyiceberg import (
    connect_to_catalog,
    read_table_data_batched
)

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def load_tables(catalog, namespace: str = "sandbox"):
    """Загрузить все таблицы."""
    users = catalog.load_table(f"{namespace}.users")
    products = catalog.load_table(f"{namespace}.products")
    orders = catalog.load_table(f"{namespace}.orders")

    return users, products, orders


def query_1_top_customers_by_orders(orders_table) -> pd.DataFrame:
    """1️⃣ Топ-10 клиентов по количеству заказов."""
    logger.info("" + "=" * 60)
    logger.info("ЗАПРОС 1: Топ-10 клиентов по заказам")
    logger.info("=" * 60)

    df = read_table_data_batched(orders_table)

    top_customers = (
        df.groupby('user_id')
        .agg({
            'order_id': 'count',
            'total_amount': 'sum'
        })
        .rename(columns={'order_id': 'orders_count', 'total_amount': 'total_spent'})
        .sort_values('orders_count', ascending=False)
        .head(10)
        .reset_index()
    )
    logger.info(f"Найдено топ-10 клиентов\n{top_customers.to_string()}")
    return top_customers


def query_2_completed_orders_by_country(users_table, orders_table) -> pd.DataFrame:
    """2️⃣ Количество завершенных заказов по странам."""
    logger.info("" + "=" * 60)
    logger.info("ЗАПРОС 2: Заказы по странам")
    logger.info("=" * 60)

    users_df = read_table_data_batched(users_table, selected_fields=('user_id', 'country'))
    orders_df = read_table_data_batched(
        orders_table,
        row_filter=EqualTo("status", "completed")
    )

    merged = orders_df.merge(users_df, on='user_id', how='inner')

    by_country = (
        merged.groupby('country')
        .agg({
            'order_id': 'count',
            'total_amount': 'sum'
        })
        .rename(columns={'order_id': 'orders_count', 'total_amount': 'revenue'})
        .sort_values('revenue', ascending=False)
        .reset_index()
    )
    logger.info(f"Найдено {len(by_country)} стран\n{by_country.to_string()}")
    return by_country


def query_3_expensive_products(products_table) -> pd.DataFrame:
    """3️⃣ Топ-10 дорогих товаров категории Electronics."""
    logger.info("" + "=" * 60)
    logger.info("ЗАПРОС 3: Дорогие Electronics")
    logger.info("=" * 60)

    df = read_table_data_batched(
        products_table,
        row_filter=And(
            EqualTo("category", "Electronics"),
            GreaterThan("price", 1500.0)
        ),
        selected_fields=('product_id', 'product_name', 'brand', 'price', 'rating')
    )

    top_expensive = df.sort_values('price', ascending=False).head(10)
    logger.info(f"Найдено {len(top_expensive)} дорогих товаров\n{top_expensive.to_string()}")
    return top_expensive


def query_4_orders_by_month(orders_table) -> pd.DataFrame:
    """4️⃣ Количество заказов и выручка по месяцам 2024 года."""
    logger.info("\n" + "=" * 60)
    logger.info("📅 ЗАПРОС 4: Динамика заказов по месяцам")
    logger.info("=" * 60)

    df = read_table_data_batched(
        orders_table,
        row_filter=EqualTo("status", "completed")
    )

    df['month'] = pd.to_datetime(df['order_date']).dt.to_period('M')
    by_month = (
        df.groupby('month')
        .agg({
            'order_id': 'count',
            'total_amount': 'sum',
            'quantity': 'sum'
        })
        .rename(columns={
            'order_id': 'orders_count',
            'total_amount': 'revenue',
            'quantity': 'items_sold'
        })
        .reset_index()
    )

    by_month['month'] = by_month['month'].astype(str)
    logger.info(f"Найдено {len(by_month)} месяцев {by_month.to_string()}")
    return by_month


def query_5_popular_categories(products_table, orders_table) -> pd.DataFrame:
    """5️⃣ Популярные категории товаров по количеству заказов."""
    logger.info("" + "=" * 60)
    logger.info("ЗАПРОС 5: Популярные категории")
    logger.info("=" * 60)

    products_df = read_table_data_batched(
        products_table,
        selected_fields=('product_id', 'category', 'price')
    )
    orders_df = read_table_data_batched(
        orders_table,
        row_filter=EqualTo("status", "completed")
    )

    merged = orders_df.merge(products_df, on='product_id', how='inner')

    by_category = (
        merged.groupby('category')
        .agg({
            'order_id': 'count',
            'quantity': 'sum',
            'total_amount': 'sum'
        })
        .rename(columns={
            'order_id': 'orders_count',
            'quantity': 'items_sold',
            'total_amount': 'revenue'
        })
        .sort_values('revenue', ascending=False)
        .reset_index()
    )
    by_category['avg_order_value'] = (by_category['revenue'] / by_category['orders_count']).round(2)
    logger.info(f"Найдено {len(by_category)} категорий\n{by_category.to_string()}")
    return by_category


def query_6_large_orders(orders_table) -> pd.DataFrame:
    """6️⃣ Крупные заказы (сумма > $5000)."""
    logger.info("\n" + "=" * 60)
    logger.info("ЗАПРОС 6: Крупные заказы")
    logger.info("=" * 60)

    df = read_table_data_batched(
        orders_table,
        row_filter=And(
            EqualTo("status", "completed"),
            GreaterThan("total_amount", 5000.0)
        )
    )
    large_orders = df.sort_values('total_amount', ascending=False)
    logger.info(f"Найдено {len(large_orders)} крупных заказов")
    logger.info(f"Общая сумма: ${large_orders['total_amount'].sum():,.2f}")
    logger.info(f"Топ-10:\n{large_orders.head(10).to_string()}")
    return large_orders


def query_7_payment_methods_stats(orders_table) -> pd.DataFrame:
    """7️⃣ Статистика по способам оплаты."""
    logger.info("" + "=" * 60)
    logger.info("ЗАПРОС 7: Способы оплаты")
    logger.info("=" * 60)

    df = read_table_data_batched(
        orders_table,
        row_filter=EqualTo("status", "completed")
    )

    by_payment = (
        df.groupby('payment_method')
        .agg({
            'order_id': 'count',
            'total_amount': ['sum', 'mean']
        })
        .reset_index()
    )

    by_payment.columns = ['payment_method', 'orders_count', 'total_revenue', 'avg_order']
    by_payment = by_payment.sort_values('total_revenue', ascending=False)
    by_payment['avg_order'] = by_payment['avg_order'].round(2)
    logger.info(f"Найдено {len(by_payment)} способов оплаты {by_payment.to_string()}")
    return by_payment


def enrich_orders():
    """Обогатить orders данными из users и products."""

    logger.info("" + "=" * 60)
    logger.info("ОБОГАЩЕНИЕ ORDERS")
    logger.info("=" * 60)

    # Подключение
    catalog = connect_to_catalog()
    namespace = "sandbox"

    # Загрузить таблицы
    orders_table = catalog.load_table(f"{namespace}.orders")
    users_table = catalog.load_table(f"{namespace}.users")
    products_table = catalog.load_table(f"{namespace}.products")

    # Прочитать данные
    orders_df = read_table_data_batched(orders_table)
    users_df = read_table_data_batched(users_table)
    products_df = read_table_data_batched(products_table)

    enriched = orders_df.merge(
        users_df[['user_id', 'name', 'email', 'age', 'country']],
        on='user_id',
        how='left'
    )

    enriched = enriched.merge(
        products_df[['product_id', 'product_name', 'category', 'brand', 'price', 'rating']],
        on='product_id',
        how='left'
    )

    # Переименовать колонки
    enriched = enriched.rename(columns={
        'name': 'user_name',
        'email': 'user_email',
        'age': 'user_age',
        'country': 'user_country',
        'price': 'product_price'
    })

    # Добавить вычисляемые поля
    enriched['unit_price'] = (enriched['total_amount'] / enriched['quantity']).round(2)
    logger.info(f"{enriched.head(10).to_string()}")
    return enriched


def run_all_analytics():
    """Запустить все аналитические запросы."""

    logger.info("" + "🎯" * 30)
    logger.info("ЗАПУСК АНАЛИТИЧЕСКИХ ЗАПРОСОВ")
    logger.info("🎯" * 30 + "")

    catalog = connect_to_catalog()
    users, products, orders = load_tables(catalog)

    # Запросы
    query_1_top_customers_by_orders(orders)
    query_2_completed_orders_by_country(users, orders)
    query_3_expensive_products(products)
    query_4_orders_by_month(orders)
    query_5_popular_categories(products, orders)
    query_6_large_orders(orders)
    query_7_payment_methods_stats(orders)
    enrich_orders()


if __name__ == "__main__":
    run_all_analytics()
