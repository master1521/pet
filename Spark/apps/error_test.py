from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("File_Error_Test") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("=" * 80)
print("Тест FileNotFoundError - частая ошибка при работе с данными")
print("Открой http://localhost:4040")
print("=" * 80)

# Успешная операция
data = [("Alice", 34), ("Bob", 45)]
df = spark.createDataFrame(data, ["name", "age"])
print("Шаг 1: Создали DataFrame в памяти")
df.show()

time.sleep(3)

# Пытаемся прочитать несуществующий файл
print("Шаг 2: Пытаемся прочитать несуществующий файл...")
df_csv = spark.read.csv("/opt/spark/data/file_not_exists.csv", header=True)
df_csv.show()

spark.stop()
