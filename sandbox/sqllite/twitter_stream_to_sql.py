import tweepy
import json
import twitter_sa

# Load credentials from json file
with open("twitter_credentials.json", "r") as file:
    creds = json.load(file)

auth = tweepy.OAuthHandler(creds['CONSUMER_KEY'],creds['CONSUMER_SECRET'])
auth.set_access_token(creds['ACCESS_TOKEN'],creds['ACCESS_SECRET'])

api = tweepy.API(auth, wait_on_rate_limit=True)

classifier = twitter_sa.TwitterClassifier()

def process_tweet(tweet, classifier):
    d = {}
    d['hashtags'] = [hashtag['text'] for hashtag in tweet.entities['hashtags']]
    d['text'] = tweet.text
    d['user'] = tweet.user.screen_name
    d['user_id'] = tweet.user.id
    d['user_loc'] = tweet.user.location
    d['sentiment'] = classifier.Classify(tweet.text)
    return d

class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(process_tweet(status, classifier))

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

myStream.filter(track=['python'], is_async=False)

""" public_tweets = api.home_timeline()
for tweet in public_tweets:
    print(tweet.text)
 """