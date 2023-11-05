from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# testing two different libraries for their sentiment, both implemented below

# TB = Text Blob
def getSentTB(df):
    sent = 0
    rc = df.reviewCount()

    if rc == 0: return 0

    for index in range(rc):
        # hopefully this converts it to a string
        text = str(df.getRow('reviewText').collect()[index])
        text = TextBlob(text)
        sent += text.sentiment.polarity

    return sent/rc



def getSentVADER(df):
    sent = 0
    rc = df.reviewCount()
    sid_obj = SentimentIntensityAnalyzer()

    if rc == 0: return 0

    for index in range(rc):
        # hopefully this converts the input df to a string
        text = str(df.getRow('reviewText').collect()[index])

        #get polarity score, it saves to a dictionary
        dict = sid_obj.polarity_scores(text)

        # continuously update average sentiment
        sent += dict['compound']
    return sent/rc