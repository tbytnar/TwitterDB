from elasticsearch import Elasticsearch
import logging

def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es

if __name__ == '__main__':
  logging.basicConfig(level=logging.ERROR)

def create_index(es_object, index_name='tweets'):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "members": {
                "dynamic": "strict",
                "properties": {
                    "user": {
                        "type": "text"
                    },
                    "user_loc": {
                        "type": "text"
                    },
                    "text": {
                        "type": "text"
                    },
                    "hashtags": {
                        "type": "nested",
                        "properties": {
                            "hashtag": {"type": "text"}
                        }
                    },
                }
            }
        }
    }
    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
            created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created

def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, doc_type='tweet', body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored

def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    return res