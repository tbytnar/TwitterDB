from social_media_streamer import SocialMediaStreamer
from social_media_crawler import SocialMediaCrawler
from content_processor import ContentProcessor
import json
import multiprocessing

with open("config.json", "r") as file:
    config = json.load(file)

try:
    config # Make sure config exists
except NameError: 
    print('There is an error with config.json')
    quit()

social_media_streamer = SocialMediaStreamer(config)
social_media_crawler = SocialMediaCrawler(config)

if __name__ == '__main__':
    crawler = multiprocessing.Process(name='Crawler', target=social_media_crawler.StartTwitterCrawl(10,250,'trump'))
    streamer = multiprocessing.Process(name='Crawler', target=social_media_streamer.StartTwitterStream('trump'))
    
    streamer.start()
    crawler.start()
