from pymongo import MongoClient
import datetime
import tweepy
import json
from twython import TwythonStreamer
from twython import Twython
import csv

# Load credentials from json file
with open("twitter_credentials.json", "r") as file:
    creds = json.load(file)

# Filter out unwanted data
def process_tweet(tweet):
    d = {}
    d['tweet_id'] = tweet['id_str']
    d['user_name'] = tweet['user']['screen_name']
    d['user_location'] = tweet['user']['location']
    d['date_created'] = tweet['created_at']
    d['hashtags'] = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
    d['original_tweet'] = tweet['in_reply_to_status_id_str']
    d['content'] = tweet['text']
    d['sentiment'] = ''
    return d

# Setup Mongodb Atlas connection
client = MongoClient('mongodb+srv://tbytnar:Mlucas0313%21@moonfrogdev.w6yur.mongodb.net/sample_airbnb?retryWrites=true&w=majority')
db = client['moon_frog_dev']
posts = db.twitter_raw

# Create a class that inherits TwythonStreamer
class MyStreamer(TwythonStreamer):     

    # Received data
    def on_success(self, data):
        try:
            # Only collect tweets in English
            if data is not None and data['lang'] == 'en':
                tweet_data = process_tweet(data)
                post_id = posts.insert_one(tweet_data).inserted_id
                print('Successfully inserted record ID: ' + str(post_id))
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