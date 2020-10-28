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

class SocialMediaCrawler():

    def __init__(self,config):
        self.social_media = config['social_media']

        self.twitter_enabled = self.social_media['twitter_enabled']
        self.facebook_enabled = self.social_media['facebook_enabled']
        self.instagram_enabled = self.social_media['instagram_enabled']
        self.youtube_enabled = self.social_media['youtube_enabled']

        if self.twitter_enabled:
            twitter_keys = config['twitter_keys']
            self.twitter = Twython(twitter_keys['CONSUMER_KEY'], twitter_keys['CONSUMER_SECRET'], twitter_keys['ACCESS_TOKEN'], twitter_keys['ACCESS_SECRET'])
        #if self.facebook_enabled:
            #facebook_keys = config['facebook_keys']
            # Facebook streaming?

        #if self.instagram_enabled:
            #instagram_keys = config['instagram_keys']
            # Instagram Streaming?

        #if self.youtube_enabled:
            #youtube_keys = config['youtube_keys']
            # Youtube streaming?

        self.content_processor = ContentProcessor()
        self.content_database = ContentDatabase(config)

    def StartTwitterCrawl(self,max_attempts,total_tweets,keyword):
        # Parameters
        MAX_ATTEMPTS = max_attempts
        COUNT_OF_TWEETS_TO_BE_FETCHED = total_tweets
        QUERY_KEYWORD = keyword

        tweets = []

        print(str(datetime.datetime.now()) + " : Starting crawl for " + keyword + " tweets.")
        # Loop for searching twitter history
        for i in range(0,MAX_ATTEMPTS):
            if(COUNT_OF_TWEETS_TO_BE_FETCHED < len(tweets)):
                break # Reached our limit

            if(0 == i):
                results = self.twitter.search(q=QUERY_KEYWORD,count=100)
            else:
                results = self.twitter.search(q=QUERY_KEYWORD,include_entities='true',max_id=next_max_id,count=100)

            for result in results['statuses']:
                tweets.append(self.content_processor.process_tweet(result))

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
                    self.content_database.twitter_upsert_tweet(tweet)
            except Exception as ex:
                print(str(ex))

        # Final Report
        print(str(len(tweets)) + ' total tweets saved.')
