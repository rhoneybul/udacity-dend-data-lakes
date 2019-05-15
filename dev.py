from pyspark import SparkContext

try:
  sc = SparkContext("local", "Dev Spark")
  print('connected to spark ')
except Exception as e:
  print(f'Could not connect to the spark context. {e}')
  
