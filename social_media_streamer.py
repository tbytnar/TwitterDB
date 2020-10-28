# -*- coding: utf-8 -*-
"""
Created on Tues Oct 27 2020
@author: Tim Bytnar
"""
from twython import TwythonStreamer
from twython import Twython
from content_processor import ContentProcessor
from database_helper import ContentDatabase
import datetime

class SocialMediaStreamer():

    def __init__(self,config):
        self.social_media = config['social_media']

        self.twitter_enabled = self.social_media['twitter_enabled']
        self.facebook_enabled = self.social_media['facebook_enabled']
        self.instagram_enabled = self.social_media['instagram_enabled']
        self.youtube_enabled = self.social_media['youtube_enabled']

        if self.twitter_enabled:
            twitter_keys = config['twitter_keys']
            self.twitter_stream = TwitterStreamer(twitter_keys['CONSUMER_KEY'], twitter_keys['CONSUMER_SECRET'], twitter_keys['ACCESS_TOKEN'], twitter_keys['ACCESS_SECRET'])
            self.twitter_stream.configure(config)
        #if self.facebook_enabled:
            #facebook_keys = config['facebook_keys']
            # Facebook streaming?

        #if self.instagram_enabled:
            #instagram_keys = config['instagram_keys']
            # Instagram Streaming?

        #if self.youtube_enabled:
            #youtube_keys = config['youtube_keys']
            # Youtube streaming?

    def StartTwitterStream(self,keyword):
        print(str(datetime.datetime.now()) + " : Streaming " + keyword + " Tweets")
        self.twitter_stream.statuses.filter(track=keyword)


# Create a class that inherits TwythonStreamer
class TwitterStreamer(TwythonStreamer):     

    # Have to pass in the config to this inherited class
    def configure(self,config):
        self.content_processor = ContentProcessor()
        self.content_database = ContentDatabase(config)
        
    # Received data
    def on_success(self, data):
        try:
            # Only collect tweets in English
            if data is not None and data['lang'] == 'en':
                self.tweet_data = self.content_processor.process_tweet(data)
                self.content_database.twitter_insert_tweet(self.tweet_data)
        except Exception as ex:
            print(str(ex))

    # Problem with the API
    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()