from pymongo import MongoClient
from pymongo import ReturnDocument
from twython import Twython
import json

# Load credentials from json file
with open("D:\\Development\\TwitterDB\\sandbox\\twitter_credentials.json", "r") as file:
    creds = json.load(file)

# Instantiate from our streaming class
twitter = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'], 
                    creds['ACCESS_TOKEN'], creds['ACCESS_SECRET'])

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
    d['language'] = tweet['lang']
    return d

# Setup Mongodb Atlas connection
client = MongoClient('mongodb+srv://tbytnar:Mlucas0313%21@moonfrogdev.w6yur.mongodb.net/sample_airbnb?retryWrites=true&w=majority')
db = client['moon_frog_dev']
posts = db.twitter_raw

# Parameters
MAX_ATTEMPTS = 100
COUNT_OF_TWEETS_TO_BE_FETCHED = 50000
QUERY_KEYWORD = 'trump'

tweets = []

# Loop for searching twitter history
for i in range(0,MAX_ATTEMPTS):
    if(COUNT_OF_TWEETS_TO_BE_FETCHED < len(tweets)):
        break # Reached our limit

    if(0 == i):
        results = twitter.search(q=QUERY_KEYWORD,count=100)
    else:
        results = twitter.search(q=QUERY_KEYWORD,include_entities='true',max_id=next_max_id,count=100)

    for result in results['statuses']:
        tweets.append(process_tweet(result))

    try:
        # Parse the data returned to get max_id to be passed in consequent call.
        next_results_url_params = results['search_metadata']['next_results']
        next_max_id = next_results_url_params.split('max_id=')[1].split('&')[0]
    except:
        # No more next pages
        break

    print('Attempt #' + str(i+1) + ' of ' + str(MAX_ATTEMPTS) + ' | Tweets found so far: ' + str(len(tweets)))

# Loop for saving processed tweets to Mongo
for tweet in tweets:
    try:
        if tweet is not None:
            post = posts.find_one_and_replace({'tweet_id':tweet['tweet_id']},tweet, upsert=True, return_document=ReturnDocument.AFTER)
            post_id = post['tweet_id']
            print('Successfully saved record ID: ' + str(post['tweet_id']) + ' Created On: ' + str(post['date_created']))
    except Exception as ex:
        print(str(ex))

# Final Report
print(str(len(tweets)) + ' total tweets saved.')