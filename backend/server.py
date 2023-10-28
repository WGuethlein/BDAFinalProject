# main backend file
# websocket api? might work best

#im not really sure what this does, but its needed
import findspark
findspark.init()

#pyspark imports
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# other function imports
from mapreduce import getResult

def main():
    spark_conf = SparkConf()\
    .setAppName("YourTest")\
    .setMaster("local[*]")

    sc = SparkContext.getOrCreate(spark_conf)

    spark = SparkSession.builder\
            .master("local")\
            .appName("Colab")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()


    df = spark.read.json('data/amazon_sample.json')

    asin = "B017O9P72A"
    words = getResult(df, asin)
    words.show(10)

    #attempt second search while in same context
    asin = "B017OBSCOS"
    words = getResult(df, asin)
    words.show(10)
    


if __name__ == "__main__":
    main()