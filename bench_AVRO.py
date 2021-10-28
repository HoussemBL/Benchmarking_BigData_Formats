import pyspark
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark.sql.types import *
from time import time

def main():
    csv_docs = "/home/houssem/scala-workspace/ML_BigDATA/Grades.csv"
    spark = SparkSession.builder.appName("how to read csv file")\
            .master("local[*]") \
            .config('spark.jars', '/home/houssem/git/Benchmarking_BigData/spark-avro_2.11-2.4.4.jar')\
            .getOrCreate()


    df = spark.read.csv(csv_docs,header=True)
    newDF=df.withColumn("university",lit("MIT"))
    newDF.show(3)

    ################################writing operation ######################################
    start_time = time()
    newDF.write\
    .format("avro")\
    .mode("overwrite")\
    .save("file:/home/houssem/spark-formats/avro/grades")
    end_time = time()

    ################################reading  (ALL) operation ######################################
    start_reading_all = time()
    orc_df = spark.read.format("avro").load("file:/home/houssem/spark-formats/avro/grades")
    orc_df.show(3)
    end_reading_all= time()
    
    ################################reading (one column) operation ######################################
    start_projection_all = time()
    orc_df_projection = spark.read.format("avro").load("file:/home/houssem/spark-formats/avro/grades").select("university")
    orc_df_projection.show(3)
    end_projection_all = time()


    writing_elapsed = end_time - start_time
    reading_all_elapsed = end_reading_all - start_reading_all
    projection_all_elapsed = end_projection_all- start_projection_all


    print("time in sec required to write AVRO: {} ".format(writing_elapsed))
    print("time in sec required to read all AVRO: {} ".format(reading_all_elapsed))
    print("time in sec required to read a column from AVRO: {} ".format(projection_all_elapsed) )

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
