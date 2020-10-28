import es_helper

es = es_helper.connect_elasticsearch()

search_object = {'query': {'match': {'text': 'biden'}}}

es_helper.search(es,'tweets',search_object)
