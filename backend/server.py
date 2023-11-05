# main backend file
# websocket api? might work best

#im not really sure what this does, but its needed
import findspark
findspark.init()

#pyspark import
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, IntegerType, FloatType

# other function imports
from Item import Item
from sent import getSentVADER, getSentTB
import time


def createSpark(file_loc):
    schema = StructType([
      StructField("overall",FloatType(),True),
      StructField("vote",IntegerType(),True),
      StructField("verified",BooleanType(),True),
      StructField("reviewTime",StringType(),True),
      StructField("reviewerID",StringType(),True),
      StructField("asin",StringType(),True),
      StructField("reviewerName",StringType(),True),
      StructField("reviewText",StringType(),True),
      StructField("summary",StringType(),True),
      StructField("unixReviewTime",IntegerType(),True),
  ])

    #create the spark context and session
    spark = SparkSession.builder\
            .master("local")\
            .appName("Amazon Reviews")\
            .config('spark.ui.port', '4050')\
            .config("spark.driver.memory", "20g")\
            .getOrCreate()
            
    return spark.read.schema(schema).json(file_loc)

def testRun(item):
    start_time = time.time()
    avgSentVADER = getSentVADER(item)
    print("TIME VADER: " + str(time.time() - start_time) + "sec")

    start_time = time.time()
    avgSentTB = getSentTB(item)
    print("TIME TB: " + str(time.time() - start_time) + "sec\n")

    print("Sent VADER: " + str(avgSentVADER))
    print("Sent TB: " + str(avgSentTB) +"\n\n")

def main():
    
    #get the data
    df = createSpark('data/larger_sample.json')

    # create test items
    p1 = Item("B017O9P72A", df)
    p2 = Item("B017OBSCOS", df)
    p3 = Item("B000VV1YOY", df)

    # show the results for the given item
    #print("Amount: " + str(p1.reviewCount()))
    #p1.showReviews()

    testRun(p1)
    testRun(p2)
    testRun(p3)



if __name__ == "__main__":
    main()



