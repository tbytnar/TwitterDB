from twython import TwythonStreamer
import csv
import json
from datetime import datetime
import es_helper



es = es_helper.connect_elasticsearch()
es_helper.create_index(es)



# Load credentials from json file
with open("twitter_credentials.json", "r") as file:
    creds = json.load(file)

# Filter out unwanted data
def process_tweet(tweet):
    d = {}
    d['hashtags'] = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
    d['text'] = tweet['text']
    d['user'] = tweet['user']['screen_name']
    d['user_loc'] = tweet['user']['location']
    return d
    
    
# Create a class that inherits TwythonStreamer
class MyStreamer(TwythonStreamer):     

    # Received data
    def on_success(self, data):
        try:
            # Only collect tweets in English
            if data is not None and data['lang'] == 'en':
                tweet_data = process_tweet(data)
                es_helper.store_record(es,'tweets',tweet_data)
        except Exception as ex:
            print(str(ex))

    # Problem with the API
    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()
        
    # Save each tweet to csv file
    def save_to_csv(self, tweet):
        with open(r'saved_tweets.csv', 'a') as file:
            writer = csv.writer(file)
            writer.writerow(list(tweet.values()))

# Instantiate from our streaming class
stream = MyStreamer(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'], 
                    creds['ACCESS_TOKEN'], creds['ACCESS_SECRET'])
# Start the stream
stream.statuses.filter(track='trump')