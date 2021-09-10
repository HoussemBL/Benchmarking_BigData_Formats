import pyspark
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark.sql.types import *
from time import time


###not yet working  ----- to inspect more
def main():
    csv_docs = "/home/houssem/scala-workspace/ML_BigDATA/Grades.csv"
    spark = SparkSession.builder.appName("how to read csv file") \
        .master("local[*]") \
        .config('spark.jars', '/home/houssem/git/Benchmarking_BigData/delta-core_2.12-1.0.0.jar') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()



    df = spark.read.csv(csv_docs,header=True)
    newDF=df.withColumn("university",lit("MIT"))
    newDF.show(3)




# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
