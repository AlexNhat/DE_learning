from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("Test Spark") \
        .getOrCreate()

    print(spark)
    
    spark.sparkContext.setLogLevel("ERROR")
    spark.stop()
    
