from pyspark.sql import SparkSession
from time import sleep

spark = SparkSession.builder \
    .appName("Test Spark Application UI") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

# Держи приложение активным 5 минут для просмотра UI
print("Application running. Open http://localhost:4040 to see DAG")
sleep(600)



spark.stop()


"""
# Запустить приложение с локалки
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/test_spark_2.py

# Или так

# Зайдите в контейнер
docker exec -it spark-master bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/test_spark_2.py
"""
