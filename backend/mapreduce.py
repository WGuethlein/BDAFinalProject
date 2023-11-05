import pyspark.sql.functions as f

#filter dataframe, keep only the reviews that equal the asin input
def filter_df(df, asin):
    return df.select('asin', 'summary','reviewtext').filter(df.asin == asin)

def getReviews(df, asin):
    return df.select('reviewtext').filter(df.asin == asin)

# go through each review, completely reformat filtered dataframe to count each word
def getWords(df):

    #method of mapreduce implemented for dataframes
    # https://medium.com/@manojt2501/pyspark-using-different-spark-api-to-write-word-count-program-324378ee04c6

    return df.withColumn('word', f.explode(f.split(f.col('reviewText'), ' ')))\
        .groupBy('word').count()\
        .sort('count', ascending=False)

# the only function that is called externally
def getResult(df, asin):
    filtered_df = filter_df(df, asin)
    d = getWords(filtered_df)
    return d

