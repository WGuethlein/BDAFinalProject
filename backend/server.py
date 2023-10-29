# main backend file
# websocket api? might work best

#im not really sure what this does, but its needed
import findspark
findspark.init()

#pyspark import
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, IntegerType, FloatType


# other function imports
import time
from mapreduce import getResult


#Item class
class Item:
    # asin -> string
    # words -> df

    """
    A class to represent an item.

    ...

    Attributes
    ----------
    asin : str
        The Item ID of the searched item
    df : pySpark DF
        Dataframe in which the work will be completed on.
    words : pySpark DF
        Dataframe with two columns, words and count, provides a list of all words and their
        occurences over all reviews under the asin

    Methods
    -------
    myWords(asin, df, words=''):
        Populates "words" list using the getResult function created in mapreduce.py
    show():
        Shows the 'words' dataframe, helper function.
    """
    def __init__(self, asin, df, words=''):
        """
        Constructs all the necessary attributes for the item object.

        Parameters
        ----------
            asin : str
                The Item ID of the searched item
            df : pySpark DF
                Dataframe in which the work will be completed on.
            words : pySpark DF, optional
                Dataframe with two columns, words and count, provides a list of all words and their
                occurences over all reviews under the asin
        """
        self.asin = asin
        self.df = df
        self.words = words

    def myWords(self):
        """
        Create's a pySpark datafram with two columns, words and count.
        Get each word occurence for the current asin provided.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.words = getResult(self.df, self.asin)

    def show(self, amount=10):
        """
        Shows the amount of specified rows in the 'words' dataframe.

        Amount is optional, if it is not provided, the function will default to 10 rows

        Parameters
        ----------
        amount : int, optional
            Amount of rows to be shown.

        Returns
        -------
        None
        """
        # if the myWords function hasn't run yet... run it first
        if self.words == '':
            self.myWords()
            start = time.perf_counter()
            self.df.cache()
            end = time.perf_counter()
            print('Cache Time: ' + str(end-start))
        self.words.show(amount)

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
    df = createSpark('data/massive_sample.json')

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

