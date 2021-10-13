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
            .getOrCreate()


    df = spark.read.csv(csv_docs,header=True)
    #df.show(5)
    newDF=df.withColumn("university",lit("MIT"))
    newDF.show(3)

    ################################writing operation ######################################
    start_time = time()
    newDF.write\
    .option("orc.compress", "snappy")\
    .mode("overwrite")\
    .orc("file:/home/houssem/spark-formats/orc/grades")
    end_time = time()

    ################################reading  (ALL) operation ######################################
    start_reading_all = time()
    orc_df = spark.read.orc("file:/home/houssem/spark-formats/orc/grades")
    orc_df.show(3)
    end_reading_all= time()
    ################################reading (one column) operation ######################################
    start_projection_all = time()
    orc_df_projection = spark.read.orc("file:/home/houssem/spark-formats/orc/grades").select("university")
    orc_df_projection.show(3)
    end_projection_all = time()


 ################################ print out results ######################################
    writing_elapsed = end_time - start_time
    reading_all_elapsed = end_reading_all - start_reading_all
    projection_all_elapsed = end_projection_all- start_projection_all

    print("time in sec required to write ORC: {} ".format(writing_elapsed))
    print("time in sec required to read all ORC: {} ".format(reading_all_elapsed))
    print("time in sec required to read a column from ORC: {} ".format(projection_all_elapsed) )

    
# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
