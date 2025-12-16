from pyspark.sql import SparkSession
from time import sleep

spark = SparkSession.builder \
    .appName("TestApp") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

spark.stop()


"""
# Запустить приложение с локалки
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/test_spark.py

# Или так

# Зайдите в контейнер
docker exec -it spark-master bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/test_spark.py
"""
