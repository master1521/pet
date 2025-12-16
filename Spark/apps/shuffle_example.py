from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col
import time

spark = SparkSession.builder \
    .appName("Shuffle_Example") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("=" * 80)
print("Примеры shuffle операций в Spark")
print("Открой http://localhost:4040 → SQL → Physical Plan")
print("Ищи 'Exchange' - это shuffle операции")
print("=" * 80)

# Создаем тестовые данные
orders = [(i, f"customer_{i % 100}", f"product_{i % 50}", i * 10)
          for i in range(1, 10001)]

products = [(f"product_{i}", f"category_{i % 10}", i * 5)
            for i in range(50)]

df_orders = spark.createDataFrame(orders, ["order_id", "customer_id", "product_id", "amount"])
df_products = spark.createDataFrame(products, ["product_id", "category", "cost"])

print(f"Заказов: {df_orders.count()}")
print(f"Продуктов: {df_products.count()}")

time.sleep(3)

# 1. GroupBy - вызывает shuffle
print("=" * 80)
print("1. GroupBy - SHUFFLE операция")
print("=" * 80)
df_grouped = df_orders.groupBy("customer_id") \
    .agg(
        sum("amount").alias("total_amount"),
        count("order_id").alias("order_count")
    )

print("Топ 10 клиентов по сумме заказов:")
df_grouped.orderBy(col("total_amount").desc()).show(10)

time.sleep(5)

# 2. Join - вызывает shuffle (если не broadcast)
print("=" * 80)
print("2. Join - SHUFFLE операция")
print("=" * 80)
df_joined = df_orders.join(df_products, "product_id", "inner")

print("Пример joined данных:")
df_joined.show(10)

time.sleep(5)

# 3. OrderBy - вызывает shuffle
print("=" * 80)
print("3. OrderBy - SHUFFLE операция")
print("=" * 80)
df_sorted = df_orders.orderBy(col("amount").desc())

print("Топ 10 заказов по сумме:")
df_sorted.show(10)

time.sleep(5)

# 4. Distinct - вызывает shuffle
print("=" * 80)
print("4. Distinct - SHUFFLE операция")
print("=" * 80)
df_distinct = df_orders.select("customer_id").distinct()

print(f"Уникальных клиентов: {df_distinct.count()}")
df_distinct.show(10)

time.sleep(5)

# 5. Repartition - явный shuffle
print("=" * 80)
print("5. Repartition - ЯВНЫЙ SHUFFLE")
print("=" * 80)
print(f"Партиций до repartition: {df_orders.rdd.getNumPartitions()}")

df_repartitioned = df_orders.repartition(10, "customer_id")

print(f"Партиций после repartition: {df_repartitioned.rdd.getNumPartitions()}")
df_repartitioned.show(5)

time.sleep(5)

print("=" * 80)
print("Все shuffle операции выполнены")
print("Смотри метрики:")
print("1. http://localhost:4040 → Jobs → каждый job → DAG")
print("2. http://localhost:4040 → Stages → Shuffle Read/Write")
print("3. http://localhost:4040 → SQL → Physical Plan → Exchange")
print("=" * 80)
print("UI будет доступен еще 5 минут")

time.sleep(300)

spark.stop()
