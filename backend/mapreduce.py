## mapreduce based on asin number
#Input: asin number
#Output: list of top 20 words? word bubble created and jpg'd?
import findspark
findspark.init()
from pyspark.sql import SparkSession;
from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType

spark = SparkSession.builder.master("local").appName("reviews").getOrCreate()
sc=spark.sparkContext

# drop selected columns, hopefully to reduce memory usage
columnsToDrop = ['unixReviewTime', 'verified', 'vote', 'reviewerName', 'reviewTime']

# create dataframe for json
# I didn't know you could use pointers in Python?!
df = spark.read.json("data/amazon_sample.json").drop(*columnsToDrop)

schema = StructType([
    StructField('asin', StringType(), True),
    StructField('asin', StringType(), True)
])

# get the first few lines (default 10) of the sample
def getHead(num=10):
    df.show(num)

# show only the columns you wish, and the amount (default 10)
def getCol(col, amount=10):
    df.select(col).show(amount)

# output the distinct asin values
def getAsins():
    print(df.select('asin').distinct().show(100))

test = ['asin','summary','reviewText']

#show reviews for a given asin
#Props -> fields: columns to see in result
#         asin: amazon item ID
#         amount: amount of rows to show
def getReviews(fields, asin="B017O9P72A", amount=20):
    df.filter(df.asin == asin).select(*fields).show(20)


#getHead(20)
#getCol('asin')
#getAsins()
getReviews(test, "B017OBMC1M")