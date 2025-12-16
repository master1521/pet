from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("Executor_Error") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("Ошибка произойдет НА EXECUTOR, смотри worker logs")

def bad_udf(x):
    if x == 2:
        raise ValueError("Специально вызванная ошибка на executor!")
    return x * 10

my_udf = udf(bad_udf, IntegerType())

data = [(1,), (2,), (3,)]
df = spark.createDataFrame(data, ["id"])

df_result = df.withColumn("result", my_udf(df.id))
df_result.show()

spark.stop()


"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/error_executor.py
"""



