from pyspark import SparkContext

try:
  sc = SparkContext("spark://localhost:4040", "Dev Spark")
except Exception as e:
  print(f'Could not connect to the spark context. {e}')
  
