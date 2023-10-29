#script to turn json dataset to .parquet 


import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, IntegerType, FloatType

spark = SparkSession \
    .builder \
    .appName("JsonToParquetPysparkExample") \
    .config("spark.driver.memory", "15g")\
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


json_df = spark.read.schema(schema).json("data/All_Amazon_Review.json", multiLine=False,) 
json_df.printSchema()
json_df.write.parquet("data/full.parquet")