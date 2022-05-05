import os, sys
# from pyspark import SparkContext
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# sc = SparkContext("local", "app_1")
spark = SparkSession.builder.master("local").appName("app_1").getOrCreate()
data = [('John', 45, 'IT'), ('Sam', 40, 'HR'), ('Smith', 41, 'IT')]
rdd = spark.sparkContext.parallelize(data)

df = rdd.toDF()
print(df)
