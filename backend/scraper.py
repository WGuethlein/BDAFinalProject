## amazon web scraper

# Input: Item ASIN number
# Output: json with item information
        # item image
        # Item name
        # Item creator
        # Item Description
        # Star count
        # review count
        # store page link

amazon_url = "https://www.amazon.com/dp/"

def get_url(asin):
    return amazon_url + asin


