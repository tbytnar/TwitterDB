from pymongo import MongoClient
from pymongo import ReturnDocument

class ContentDatabase():

    def __init__(self,config):
        # Setup Mongodb Atlas connection
        client = MongoClient(config['mongo_connection'])
        db = client['moon_frog_dev']
        self.twitter_raw = db.twitter_raw

    def twitter_insert_tweet(self,tweet):
        post_id = self.twitter_raw.insert_one(tweet).inserted_id
        #print('Successfully inserted record ID: ' + str(post_id))

    def twitter_upsert_tweet(self,tweet):
        post = self.twitter_raw.find_one_and_replace({'tweet_id':tweet['tweet_id']},tweet, upsert=True, return_document=ReturnDocument.AFTER)
        #print('Successfully saved record ID: ' + str(post['tweet_id']) + ' Created On: ' + str(post['date_created']))
    
    def twitter_get_all_raw(self,filter):
        return list(self.twitter_raw.find(filter))
