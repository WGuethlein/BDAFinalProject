from mapreduce import getResult, getReviews
import time
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer


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
    def __init__(self, asin, df, words='', reviews=''):
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
        self.reviews = reviews

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
        if(not self.df.storageLevel.useMemory):
            self.df.cache()

    def myReviews(self):
        self.reviews = getReviews(self.df, self.asin)
        if(not self.df.storageLevel.useMemory):
            self.df.cache()

    def reviewCount(self):
        if self.reviews == '':
            self.myReviews()
        return self.reviews.count()

    def showReviews(self,amount=10):
        self.myReviews()
        assert self.reviews != '', "the reviews should be filled before getting Sentiment, run the myReviews() function."
        
        self.reviews.show(amount)

    def getRow(self, column = 'reviewText'):
        return self.df.select(column)


    def showWords(self, amount=10):
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
        assert self.words != '', "the wordList should be filled before getting Sentiment, run the myWords() function."
        
        self.words.show(amount)

    def getSentiment(self):
        assert self.words != '', "the wordList should be filled before getting Sentiment, run the myWords() function."

        blob = TextBlob(self.words, analyzer=NaiveBayesAnalyzer())
        return blob.sentiment