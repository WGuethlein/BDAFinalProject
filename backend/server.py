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

def main():
    
    #get the data
    df = createSpark('data/large_sample.json')

    # create test items
    p1 = Item("B017O9P72A", df)
    p2 = Item("B017OBSCOS", df)
    p3 = Item("B000VV1YOY", df)

    # show the results for the given item
    p1.show()
    p2.show()
    p3.show()


if __name__ == "__main__":
    main()

