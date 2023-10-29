## mapreduce based on asin number
#Input: asin number
#Output: list of top 20 words? word bubble created and jpg'd?

import time

import pyspark.sql.functions as f


#filter dataframe, keep only the reviews that equal the asin input
def filter_df(df, asin):
    return df.select('asin', 'summary','reviewtext').filter(df.asin == asin)

# go through each review, completely reformat filtered dataframe to count each word
def getWords(df):
    return df.withColumn('word', f.explode(f.split(f.col('reviewText'), ' ')))\
    .groupBy('word').count()\
    .sort('count', ascending=False)

def getResult(df, asin):
    filtered_df = filter_df(df, asin)
    d = getWords(filtered_df)

    return d

