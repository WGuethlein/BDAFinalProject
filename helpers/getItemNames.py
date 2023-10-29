import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, IntegerType, FloatType

spark = SparkSession \
    .builder \
    .appName("itemNames") \
    .config("spark.driver.memory", "10g")\
    .getOrCreate()

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


json_df = spark.read.schema(schema).json("data/large_sample.json", multiLine=False) 


def getWords(df):
    return df.select("asin").rdd.flatMap(lambda x: x).collect()

df = getWords(json_df)
print(df)