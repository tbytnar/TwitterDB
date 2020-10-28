import pickle
import nltk

from nltk.tag import pos_tag
from nltk.corpus import twitter_samples
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
import re, string
from nltk.tokenize import word_tokenize

class ContentProcessor():

    def process_tweet(self,tweet):
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

    def remove_noise(self, tweet_tokens, stop_words = ()):

        cleaned_tokens = []

        for token, tag in pos_tag(tweet_tokens):
            token = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|'\
                        '(?:%[0-9a-fA-F][0-9a-fA-F]))+','', token)
            token = re.sub("(@[A-Za-z0-9_]+)","", token)

            if tag.startswith("NN"):
                pos = 'n'
            elif tag.startswith('VB'):
                pos = 'v'
            else:
                pos = 'a'

            lemmatizer = WordNetLemmatizer()
            token = lemmatizer.lemmatize(token, pos)

            if len(token) > 0 and token not in string.punctuation and token.lower() not in stop_words:
                cleaned_tokens.append(token.lower())
        return cleaned_tokens

    def classify_tweet(self,tweet):
        filename = 'finalized_model.sav'
        classifier = pickle.load(open(filename, 'rb'))
        tweet_tokens = self.remove_noise(word_tokenize(tweet['content']))
        result = classifier.classify(dict([token, True] for token in tweet_tokens))
        #print("Finished classifying tweet " + str(tweet['tweet_id']) + " it's " + str(result))
        return result