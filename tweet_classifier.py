from content_processor import ContentProcessor
from database_helper import ContentDatabase
import json

with open("config.json", "r") as file:
    config = json.load(file)

try:
    config # Make sure config exists
except NameError: 
    print('There is an error with config.json')
    quit()

content_processor = ContentProcessor()
content_database = ContentDatabase(config)

if __name__ == '__main__':  
    tweets = content_database.twitter_get_all_raw({"sentiment":""}) # Getting all unclassified tweets
    total_tweets = len(tweets)
    counter = 1

    for tweet in tweets:
        tweet['sentiment'] = content_processor.classify_tweet(tweet)
        content_database.twitter_upsert_tweet(tweet)
        remaining = total_tweets - counter
        counter = counter + 1
        print("Tweets remaining: " + str(remaining))
